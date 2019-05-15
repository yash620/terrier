#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier {
// Not thread-safe
class MVCCDataTableTestObject {
 public:
  template <class Random>
  MVCCDataTableTestObject(storage::BlockStore *block_store, const uint16_t max_col, Random *generator)
      : layout_(StorageTestUtil::RandomLayoutNoVarlen(max_col, generator)),
        table_(block_store, layout_, storage::layout_version_t(0)) {}

  ~MVCCDataTableTestObject() {
    for (auto ptr : loose_pointers_) delete[] ptr;
    for (auto ptr : loose_txns_) delete ptr;
    delete[] select_buffer_;
  }

  const storage::BlockLayout &Layout() const { return layout_; }

  template <class Random>
  storage::ProjectedRow *GenerateRandomTuple(Random *generator) {
    auto *buffer = common::AllocationUtil::AllocateAligned(redo_initializer.ProjectedRowSize());
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow *redo = redo_initializer.InitializeRow(buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);
    return redo;
  }

  template <class Random>
  storage::ProjectedRow *GenerateRandomUpdate(Random *generator) {
    std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    storage::ProjectedRowInitializer update_initializer =
        storage::ProjectedRowInitializer::CreateProjectedRowInitializer(layout_, update_col_ids);
    auto *buffer = common::AllocationUtil::AllocateAligned(update_initializer.ProjectedRowSize());
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow *update = update_initializer.InitializeRow(buffer);
    StorageTestUtil::PopulateRandomRow(update, layout_, null_bias_, generator);
    return update;
  }

  storage::ProjectedRow *GenerateVersionFromUpdate(const storage::ProjectedRow &delta,
                                                   const storage::ProjectedRow &previous) {
    auto *buffer = common::AllocationUtil::AllocateAligned(redo_initializer.ProjectedRowSize());
    loose_pointers_.push_back(buffer);
    // Copy previous version
    std::memcpy(buffer, &previous, redo_initializer.ProjectedRowSize());
    auto *version = reinterpret_cast<storage::ProjectedRow *>(buffer);
    std::unordered_map<uint16_t, uint16_t> col_to_projection_list_index;
    storage::StorageUtil::ApplyDelta(layout_, delta, version);
    return version;
  }

  storage::ProjectedRow *SelectIntoBuffer(transaction::TransactionContext *const txn, const storage::TupleSlot slot) {
    // generate a redo ProjectedRow for Select
    storage::ProjectedRow *select_row = redo_initializer.InitializeRow(select_buffer_);
    select_result_ = table_.Select(txn, slot, select_row);
    return select_row;
  }

  storage::BlockLayout layout_;
  storage::DataTable table_;
  // We want null_bias_ to be zero when testing CC. We already evaluate null correctness in other directed tests, and
  // we don't want the logically deleted field to end up set NULL.
  const double null_bias_ = 0;
  std::vector<byte *> loose_pointers_;
  std::vector<transaction::TransactionContext *> loose_txns_;
  storage::ProjectedRowInitializer redo_initializer = storage::ProjectedRowInitializer::CreateProjectedRowInitializer(
      layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
  byte *select_buffer_ = common::AllocationUtil::AllocateAligned(redo_initializer.ProjectedRowSize());
  bool select_result_;
};

class ConstraintTests : public ::terrier::TerrierTest {
 public:
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{10000, 10000};
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 100;
  const uint16_t max_columns_ = 100;

  bool TransactionAborted(uint64_t timestamp) { return (timestamp & ((static_cast<uint64_t>(1))  << 63)) > 0; }
};

// NOLINTNEXTLINE
TEST_F(ConstraintTests, ConstraintFailNotEnforcing) {
  transaction::TransactionManager txn_manager(&buffer_pool_, false, LOGGING_DISABLED);
  MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

  auto *txn0 = txn_manager.BeginTransaction();
  auto *txn1 = txn_manager.BeginTransaction();

  tested.loose_txns_.push_back(txn0);
  tested.loose_txns_.push_back(txn1);

  auto *insert_tuple_0 = tested.GenerateRandomTuple(&generator_);
  auto *insert_tuple_1 = tested.GenerateRandomTuple(&generator_);
  tested.table_.Insert(txn0, *insert_tuple_0);
  tested.table_.Insert(txn1, *insert_tuple_1);

  txn_manager.InstallConstraint(txn0, [&]() -> bool { return false; });

  auto result1 = txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);
  auto result0 = txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

  ASSERT_TRUE(TransactionAborted(!result0));   // txn_0 should abort
  ASSERT_FALSE(TransactionAborted(!result1));  // txn_1 should commit
}

// NOLINTNEXTLINE
TEST_F(ConstraintTests, ConstraintFailEnforcing) {
  transaction::TransactionManager txn_manager(&buffer_pool_, false, LOGGING_DISABLED);

  MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

  auto *txn0 = txn_manager.BeginTransaction();
  auto *txn1 = txn_manager.BeginTransaction();

  tested.loose_txns_.push_back(txn0);
  tested.loose_txns_.push_back(txn1);

  auto *insert_tuple_0 = tested.GenerateRandomTuple(&generator_);
  auto *insert_tuple_1 = tested.GenerateRandomTuple(&generator_);
  tested.table_.Insert(txn0, *insert_tuple_0);
  tested.table_.Insert(txn1, *insert_tuple_1);

  txn_manager.InstallConstraint(txn0, [&]() -> bool { return false; });

  auto result0 = txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);
  auto result1 = txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

  ASSERT_TRUE(TransactionAborted(!result1));   // txn_1 should abort
  ASSERT_FALSE(TransactionAborted(!result0));  // txn_0 should commit
}

// NOLINTNEXTLINE
TEST_F(ConstraintTests, ConstraintPassing) {
  transaction::TransactionManager txn_manager(&buffer_pool_, false, LOGGING_DISABLED);

  MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

  auto *txn0 = txn_manager.BeginTransaction();
  auto *txn1 = txn_manager.BeginTransaction();

  tested.loose_txns_.push_back(txn0);
  tested.loose_txns_.push_back(txn1);

  auto *insert_tuple_0 = tested.GenerateRandomTuple(&generator_);
  auto *insert_tuple_1 = tested.GenerateRandomTuple(&generator_);
  tested.table_.Insert(txn0, *insert_tuple_0);
  tested.table_.Insert(txn1, *insert_tuple_1);

  txn_manager.InstallConstraint(txn0, [&]() -> bool { return true; });

  auto result0 = txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);
  auto result1 = txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

  ASSERT_FALSE(TransactionAborted(!result1));  // txn_1 should commit
  ASSERT_FALSE(TransactionAborted(!result0));  // txn_0 should commit
}

}  // namespace terrier
