#pragma once

#include <velox/exec/TaskStats.h>

#include "Query.h"
#include "velox4j/iterator/UpIterator.h"
#include "velox4j/memory/MemoryManager.h"

namespace velox4j {

class SerialTaskStats {
 public:
  SerialTaskStats(const facebook::velox::exec::TaskStats& taskStats);

  folly::dynamic toJson() const;

 private:
  facebook::velox::exec::TaskStats taskStats_;
};

class SerialTask : public UpIterator {
 public:
  SerialTask(MemoryManager* memoryManager, std::shared_ptr<const Query> query);

  ~SerialTask() override;

  State advance() override;

  void wait() override;

  facebook::velox::RowVectorPtr get() override;

  void addSplit(
      const facebook::velox::core::PlanNodeId& planNodeId,
      int32_t groupId,
      std::shared_ptr<facebook::velox::connector::ConnectorSplit>
          connectorSplit);

  void noMoreSplits(const facebook::velox::core::PlanNodeId& planNodeId);

  std::unique_ptr<SerialTaskStats> collectStats();

 private:
  State advance0(bool wait);

  void saveDrivers();

  MemoryManager* const memoryManager_;
  std::shared_ptr<const Query> query_;
  std::shared_ptr<facebook::velox::exec::Task> task_;
  std::vector<std::shared_ptr<facebook::velox::exec::Driver>> drivers_{};
  bool hasPendingState_{false};
  State pendingState_{State::BLOCKED};
  facebook::velox::RowVectorPtr pending_{nullptr};
};

class QueryExecutor {
 public:
  QueryExecutor(
      MemoryManager* memoryManager,
      std::shared_ptr<const Query> query);

  std::unique_ptr<SerialTask> execute() const;

 private:
  MemoryManager* const memoryManager_;
  const std::shared_ptr<const Query> query_;
};

} // namespace velox4j
