/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleExchangeSource.h"
#include "velox/common/Casts.h"
#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/row/CompactRow.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

ShuffleRead::ShuffleRead(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const ShuffleReadNode>& shuffleReadNode,
    std::shared_ptr<InMemoryExchangeClient> exchangeClient)
    : Exchange(
          operatorId,
          ctx,
          shuffleReadNode,
          std::move(exchangeClient),
          "ShuffleRead") {
  initStats();
}

void ShuffleRead::initStats() {
  VELOX_CHECK(runtimeStats_.empty());
  runtimeStats_.insert(
      std::pair{kShuffleDecodeTime, velox::RuntimeCounter::Unit::kNanos});
  runtimeStats_.insert(
      std::pair{
          kShufflePagesPerInputBatch, velox::RuntimeCounter::Unit::kNone});
}

void ShuffleRead::resetOutputState() {
  currentPages_.clear();
  rows_.clear();
  pageRows_.clear();
  nextRow_ = 0;
  nextPage_ = 0;
}

RowVectorPtr ShuffleRead::getOutput() {
  if (currentPages_.empty()) {
    return nullptr;
  }

  SCOPE_EXIT {
    if (nextRow_ == rows_.size()) {
      VELOX_CHECK_EQ(nextPage_, currentPages_.size());
      resetOutputState();
    }
  };

  uint64_t rawInputBytes{0};
  if (rows_.empty()) {
    VELOX_CHECK_EQ(nextRow_, 0);
    size_t numRows{0};
    for (const auto& page : currentPages_) {
      auto* batch = checkedPointerCast<ShuffleSerializedPage>(page.get());
      VELOX_CHECK_LE(batch->size(), std::numeric_limits<int32_t>::max());
      rawInputBytes += page->size();
      const auto pageRows = page->numRows().value();
      pageRows_.emplace_back(
          (pageRows_.empty() ? 0 : pageRows_.back()) + pageRows);
      numRows += pageRows;
    }
    rows_.reserve(numRows);
    const int32_t driverId = operatorCtx()->driverCtx()->driverId;
    for (const auto& page : currentPages_) {
      auto* batch = checkedPointerCast<ShuffleSerializedPage>(page.get());
      const auto& rows = batch->rows(driverId);
      for (const auto& row : rows) {
        rows_.emplace_back(row);
      }
    }
    if (!currentPages_.empty()) {
      runtimeStats_[kShufflePagesPerInputBatch].addValue(currentPages_.size());
      ++numInputBatches_;
    }
  }
  VELOX_CHECK_LE(nextRow_, rows_.size());
  if (rows_.empty()) {
    return nullptr;
  }

  auto numOutputRows = kInitialOutputRows;
  if (estimatedRowSize_.has_value()) {
    numOutputRows = std::max(
        (preferredOutputBatchBytes_ / estimatedRowSize_.value()),
        kInitialOutputRows);
  }
  numOutputRows = std::min<uint64_t>(numOutputRows, rows_.size() - nextRow_);

  uint64_t decodeTimeNs{0};
  {
    velox::NanosecondTimer timer(&decodeTimeNs);
    // Create a view of the rows to deserialize from nextRow_ to nextRow_ +
    // numOutputRows.
    if (numOutputRows == rows_.size()) {
      result_ = row::CompactRow::deserialize(rows_, outputType_, pool());
    } else {
      std::vector<std::string_view> outputRows(
          rows_.begin() + nextRow_, rows_.begin() + nextRow_ + numOutputRows);
      result_ = row::CompactRow::deserialize(outputRows, outputType_, pool());
    }
  }
  runtimeStats_[kShuffleDecodeTime].addValue(decodeTimeNs);

  nextRow_ += numOutputRows;
  for (; nextPage_ < currentPages_.size(); ++nextPage_) {
    if (pageRows_[nextPage_] > nextRow_) {
      break;
    }
    currentPages_[nextPage_].reset();
  }
  estimatedRowSize_ = std::max(
      result_->estimateFlatSize() / numOutputRows,
      estimatedRowSize_.value_or(1L));
  recordInputStats(rawInputBytes);
  return result_;
}

void ShuffleRead::close() {
  Exchange::close();
  auto lockedStats = stats_.wlock();
  for (const auto& [name, metric] : runtimeStats_) {
    if (metric.count == 0) {
      continue;
    }
    lockedStats->runtimeStats[name] = metric;
  }
  if (numInputBatches_ != 0) {
    lockedStats->addRuntimeStat(
        kShuffleInputBatches, RuntimeCounter(numInputBatches_));
  }
}

folly::dynamic ShuffleReadNode::serialize() const {
  // Deliberately PlanNode::serialize(), not ExchangeNode::serialize(): the
  // serde kind and transport id are constants of this class rather than data,
  // so the serialized form stays exactly what it was before ShuffleReadNode
  // became an ExchangeNode.
  auto obj = PlanNode::serialize();
  obj["outputType"] = outputType()->serialize();
  return obj;
}

velox::core::PlanNodePtr ShuffleReadNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<ShuffleReadNode>(
      deserializePlanNodeId(obj),
      ISerializable::deserialize<RowType>(obj["outputType"], context));
}

void registerShuffleReadTransport() {
  const auto inMemory = ExchangeTransportRegistry::tryGet(
      std::string{core::TransportKind::kInMemory});
  VELOX_CHECK_NOT_NULL(
      inMemory,
      "ShuffleRead requires the built-in in-memory exchange transport");

  auto entry = std::make_shared<ExchangeTransportEntry>();
  // The data plane is the in-memory one -- only the ExchangeSource differs, and
  // that is chosen per URL scheme -- so reuse its client factory rather than
  // duplicating the query-config plumbing it reads.
  entry->makeClient = inMemory->makeClient;
  entry->makeOperator =
      [](int32_t operatorId,
         DriverCtx* ctx,
         const std::shared_ptr<const core::ExchangeNode>& node,
         std::shared_ptr<ExchangeClient> client) -> std::unique_ptr<Operator> {
    auto shuffleReadNode =
        std::dynamic_pointer_cast<const ShuffleReadNode>(node);
    VELOX_CHECK_NOT_NULL(
        shuffleReadNode,
        "Transport '{}' is reserved for ShuffleReadNode, got plan node: {}",
        ShuffleReadNode::kTransportKind,
        node->id());
    auto inMemoryClient =
        std::dynamic_pointer_cast<InMemoryExchangeClient>(std::move(client));
    VELOX_CHECK_NOT_NULL(
        inMemoryClient, "ShuffleRead requires an InMemoryExchangeClient");
    return std::make_unique<ShuffleRead>(
        operatorId, ctx, shuffleReadNode, std::move(inMemoryClient));
  };
  // 'overwrite' keeps a second call a no-op instead of throwing on the
  // duplicate key.
  ExchangeTransportRegistry::global().insert(
      std::string{ShuffleReadNode::kTransportKind},
      std::move(entry),
      /*overwrite=*/true);
}
} // namespace facebook::presto::operators
