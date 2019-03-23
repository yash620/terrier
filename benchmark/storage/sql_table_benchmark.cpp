#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/strong_typedef.h"
#include "storage/data_table.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/multithread_test_util.h"
#include "util/storage_test_util.h"

namespace terrier {

// This benchmark simulates a key-value store inserting a large number of tuples. This provides a good baseline and
// reference to other fast data structures (indexes) to compare against. We are interested in the SqlTable's raw
// performance, so the tuple's contents are intentionally left garbage and we don't verify correctness. That's the job
// of the Google Tests.

class SqlTableBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    common::WorkerPool thread_pool(num_threads_, {});

    // create schema
    catalog::col_oid_t col_oid(0);
    for (uint32_t i = 0; i < column_num_; i++) {
      columns_.emplace_back("", type::TypeId::BIGINT, false, col_oid++);
    }
    schema_ = new catalog::Schema(columns_, storage::layout_version_t(0));
    table_ = new storage::SqlTable(&block_store_, *schema_, catalog::table_oid_t(0));

    //
    std::vector<catalog::col_oid_t> all_col_oids;
    for (auto &col : schema_->GetColumns()) all_col_oids.emplace_back(col.GetOid());
    auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(0));

    initializer_ = new storage::ProjectedRowInitializer(std::get<0>(pair));
    map_ = new storage::ProjectionMap(std::get<1>(pair));
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_->ProjectedRowSize());
    redo_ = initializer_->InitializeRow(redo_buffer_);
    CatalogTestUtil::PopulateRandomRow(redo_, *schema_, pair.second, &generator_);

    // generate a ProjectedRow buffer to Read
    read_buffer_ = common::AllocationUtil::AllocateAligned(initializer_->ProjectedRowSize());
    read_ = initializer_->InitializeRow(read_buffer_);
  }

  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
    delete[] read_buffer_;
    delete schema_;
    delete initializer_;
    delete map_;
    delete table_;
    columns_.clear();
  }

  // Sql Table
  storage::SqlTable *table_ = nullptr;

  // Tuple properties
  const storage::ProjectedRowInitializer *initializer_ = nullptr;
  const storage::ProjectionMap *map_;

  // Workload
  const uint32_t num_inserts_ = 10000000;
  const uint32_t num_reads_ = 10000000;
  const uint32_t num_threads_ = 4;
  const uint64_t buffer_pool_reuse_limit_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{num_inserts_, buffer_pool_reuse_limit_};

  // Schema
  const uint32_t column_num_ = 2;
  std::vector<catalog::Schema::Column> columns_;
  catalog::Schema *schema_ = nullptr;

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  // Read buffer pointers;
  byte *read_buffer_;
  storage::ProjectedRow *read_;
};

// Insert the num_inserts_ of tuples into a SqlTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SimpleInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // We can use dummy timestamps here since we're not invoking concurrency control
    transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                        LOGGING_DISABLED);
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Insert(&txn, *redo_, storage::layout_version_t(0));
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Insert the num_inserts_ of tuples into a SqlTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, ConcurrentInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                          LOGGING_DISABLED);
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; ++i) {
        table_->Insert(&txn, *redo_, storage::layout_version_t(0));
      }
    };
    common::WorkerPool thread_pool(num_threads_, {});
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a sequential order from a SqlTable in a single thread
// The SqlTable has only one schema version
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SingleVersionSequentialRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_, *map_, storage::layout_version_t(0));
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a sequential order from a SqlTable in a single thread
// The SqlTable has multiple schema versions and the read version mismatches the one stored in the storage layer
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMismatchSequentialRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }
  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schame(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schame);

  // create a new read buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *read_pr = pair.first.InitializeRow(buffer);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_pr, pair.second, storage::layout_version_t(1));
    }
  }
  delete[] buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a SqlTable in a single thread
// The SqlTable has only one schema version
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, SingleVersionRandomRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }
  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_, *map_, storage::layout_version_t(0));
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a SqlTable in a single thread
// The SqlTable has multiple schema versions and the read version mismatches the one stored in the storage layer
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SqlTableBenchmark, MultiVersionMismatchRandomRead)(benchmark::State &state) {
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0), &buffer_pool_,
                                      LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_->Insert(&txn, *redo_, storage::layout_version_t(0)));
  }
  // create new schema
  catalog::col_oid_t col_oid(column_num_);
  std::vector<catalog::Schema::Column> new_columns(columns_.begin(), columns_.end() - 1);
  new_columns.emplace_back("", type::TypeId::BIGINT, false, col_oid);
  catalog::Schema new_schame(new_columns, storage::layout_version_t(1));
  table_->UpdateSchema(new_schame);

  // create a new read buffer
  std::vector<catalog::col_oid_t> all_col_oids(new_columns.size());
  for (size_t i = 0; i < new_columns.size(); i++) all_col_oids[i] = new_columns[i].GetOid();
  auto pair = table_->InitializerForProjectedRow(all_col_oids, storage::layout_version_t(1));
  byte *buffer = common::AllocationUtil::AllocateAligned(pair.first.ProjectedRowSize());
  storage::ProjectedRow *read_pr = pair.first.InitializeRow(buffer);

  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table_->Select(&txn, read_order[i], read_pr, pair.second, storage::layout_version_t(1));
    }
  }
  delete[] buffer;
  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

BENCHMARK_REGISTER_F(SqlTableBenchmark, SimpleInsert)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionSequentialRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchSequentialRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, SingleVersionRandomRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(SqlTableBenchmark, MultiVersionMismatchRandomRead)->Unit(benchmark::kMillisecond);
}  // namespace terrier
