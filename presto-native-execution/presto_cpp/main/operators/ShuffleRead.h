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
class ShuffleReadNode : public velox::core::ExchangeNode {
 public:
  /// Transport id under which ShuffleRead registers its operator. The wire
  /// transport is the in-memory one; this id exists so the exchange transport
  /// registry resolves to ShuffleRead rather than to a plain Exchange.
  static constexpr std::string_view kTransportKind{"presto-shuffle-read"};

  ShuffleReadNode(const velox::core::PlanNodeId& id, velox::RowTypePtr type)
      : ExchangeNode(
            id,
            std::move(type),
            "CompactRow",
            std::string{kTransportKind}) {}

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const ShuffleReadNode& other) {
      id_ = other.id();
      outputType_ = other.outputType();
    }

    Builder& id(velox::core::PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& outputType(velox::RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    std::shared_ptr<ShuffleReadNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ShuffleReadNode id is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "ShuffleReadNode outputType is not set");

      return std::make_shared<ShuffleReadNode>(
          id_.value(), outputType_.value());
    }

   private:
    std::optional<velox::core::PlanNodeId> id_;
    std::optional<velox::RowTypePtr> outputType_;
  };

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  std::string_view name() const override {
    return "ShuffleRead";
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    // Nothing to add
  }
};

class ShuffleRead : public velox::exec::Exchange {
 public:
  ShuffleRead(
      int32_t operatorId,
      velox::exec::DriverCtx* ctx,
      const std::shared_ptr<const ShuffleReadNode>& shuffleReadNode,
      std::shared_ptr<velox::exec::InMemoryExchangeClient> exchangeClient);

  velox::RowVectorPtr getOutput() override;

  void close() override;

  static inline const std::string kShuffleDecodeTime{"shuffleDecodeWallNanos"};
  static inline const std::string kShufflePagesPerInputBatch{
      "shuffleNumPagesPerInputBatch"};
  static inline const std::string kShuffleInputBatches{"shuffleInputBatches"};

 protected:
  velox::VectorSerde* getSerde() override {
    VELOX_UNSUPPORTED("ShuffleReadOperator doesn't use serde");
  }

 private:
  void initStats();

  void resetOutputState();

  int64_t numInputBatches_{0};
  std::unordered_map<std::string, velox::RuntimeMetric> runtimeStats_;

  size_t nextRow_{0};
  size_t nextPage_{0};
  // Reusable buffers.
  std::vector<std::string_view> rows_;
  std::vector<size_t> pageRows_;
};

/// Registers the ShuffleRead exchange transport under
/// ShuffleReadNode::kTransportKind, pairing the built-in in-memory exchange
/// client with the ShuffleRead operator. Must run before any task is created.
/// Idempotent.
void registerShuffleReadTransport();
} // namespace facebook::presto::operators
