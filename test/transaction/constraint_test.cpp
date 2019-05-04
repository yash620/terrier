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
};

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    COMMIT |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because its start time is before #0's commit
// Txn #2 should only read Txn #0's version of X
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
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

  ASSERT_EQ(!result0, 0);      // txn_0 should abort
  ASSERT_TRUE(!result1 != 0);  // txn_1 should commit
}

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

  auto constraint = txn_manager.InstallConstraint(txn0, [&]() -> bool { return false; });

  constraint->SetEnforcing();

  auto result1 = txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);
  auto result0 = txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

  ASSERT_EQ(!result1, 0);      // txn_1 should abort
  ASSERT_TRUE(!result0 != 0);  // txn_0 should commit
}

TEST_F(ConstraintTests, ConstraintPassingNotEnforcing) {
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

  ASSERT_TRUE(!result0 != 0);  // txn_0 should commit
  ASSERT_TRUE(!result1 != 0);  // txn_1 should commit
}

TEST_F(ConstraintTests, ConstraintPassingEnforcing) {
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

  auto constraint = txn_manager.InstallConstraint(txn0, [&]() -> bool { return true; });
  constraint->SetEnforcing();

  auto result0 = txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);
  auto result1 = txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

  ASSERT_TRUE(!result0 != 0);  // txn_0 should commit
  ASSERT_TRUE(!result1 != 0);  // txn_1 should commit
}

}  // namespace terrier