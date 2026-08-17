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
#pragma once

#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/InMemoryExchangeClient.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

/// Plan node for reading shuffle data written by MaterializedOutput.
/// Paired with MaterializedOutputNode for symmetric A/B switching.
///
/// An ExchangeNode, so the runtime creates an exchange client for it and
/// resolves its operator from the exchange transport registry. Being a leaf,
/// its data arrives through that client from splits (RemoteConnectorSplit)
/// rather than from upstream operators.
class MaterializedExchangeNode : public velox::core::ExchangeNode {
 public:
  /// Transport id under which MaterializedExchange registers its operator. The
  /// wire transport is the in-memory one; this id exists so the exchange
  /// transport registry resolves to MaterializedExchange rather than to a plain
  /// Exchange.
  static constexpr std::string_view kTransportKind{
      "presto-materialized-exchange"};

  MaterializedExchangeNode(
      const velox::core::PlanNodeId& id,
      velox::RowTypePtr outputType)
      : ExchangeNode(
            id,
            std::move(outputType),
            "CompactRow",
            std::string{kTransportKind}) {}

  std::string_view name() const override {
    return "MaterializedExchange";
  }

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& /* stream */) const override {}
};

/// Operator for reading shuffle data written by MaterializedOutput.
///
/// Reads pages from ExchangeClient (via ShuffleExchangeSource), strips the
/// kFormatBatched prefix, and parses RowGroupHeader + TRowSize-framed
/// CompactRow data into RowVectors. Only handles batched format — no
/// kFormatRaw/legacy support (that's in ShuffleRead).
///
/// Deserializes CompactRow data directly via expandBatchedPage() using the
/// fixed RowGroupHeader + TRowSize framing, bypassing the VectorSerde
/// abstraction. This avoids VectorStreamGroup's column-by-column
/// deserialization overhead since the framing format is fixed and known
/// at compile time (written by MaterializedOutput).
class MaterializedExchange : public velox::exec::Exchange {
 public:
  static constexpr std::string_view kInputBatches =
      "materializedExchangeInputBatches";
  static constexpr std::string_view kTotalRows =
      "materializedExchangeTotalRows";

  MaterializedExchange(
      int32_t operatorId,
      velox::exec::DriverCtx* ctx,
      const std::shared_ptr<const MaterializedExchangeNode>&
          materializedExchangeNode,
      std::shared_ptr<velox::exec::InMemoryExchangeClient> exchangeClient);

  velox::RowVectorPtr getOutput() override;

  void close() override;

 private:
  // Not used — MaterializedExchange deserializes CompactRow directly via
  // expandBatchedPage(), bypassing the VectorSerde abstraction.
  velox::VectorSerde* getSerde() override {
    VELOX_UNSUPPORTED("MaterializedExchange doesn't use serde");
  }

  // Parse RowGroupHeader + TRowSize-framed CompactRow data written by
  // MaterializedOutput into row string_views. Operates directly on the framed
  // byte layout (RowGroupHeader followed by big-endian TRowSize + row bytes)
  // rather than going through VectorStreamGroup deserialization, since the
  // framing format is fixed and known at compile time.
  void expandBatchedPage(std::string_view pageData);

  // Clear accumulated page and row state after all rows are consumed.
  void resetOutputState();

  // Expand all current pages into row string_views.
  uint64_t parseCurrentPages();

  // Compute output batch size and deserialize rows into a RowVector.
  velox::RowVectorPtr deserializeNextBatch();

  // Cumulative stats.
  int64_t numInputBatches_{0};
  int64_t totalRows_{0};

  // Row parsing state — populated by parseCurrentPages(), consumed by
  // deserializeNextBatch(). Reset when all rows are consumed.
  std::vector<std::string_view> rows_;
  size_t nextRow_{0};
};

/// Registers the MaterializedExchange transport under
/// MaterializedExchangeNode::kTransportKind, pairing the built-in in-memory
/// exchange client with the MaterializedExchange operator. Must run before any
/// task is created. Idempotent.
void registerMaterializedExchangeTransport();

} // namespace facebook::presto::operators
