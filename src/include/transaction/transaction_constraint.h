#pragma once
#include "transaction/transaction_defs.h"

namespace terrier::transaction {
/**
 * A constraint that needs to be checked before a transaction can commit
 * Only Updating Txns will install or check constraints
 */
class TransactionConstraint {
 public:
  /**
   * Initializes a new TransactionConstraint.
   * @param install_time The original start_time of the transaction installing constraint
   * @param installing_txn_id The txn_id of the txn installing this constraint
   * @param verify_fn The function run to verify whether constraint is satisfied, returns true if constraint satisfied
   */
  TransactionConstraint(const timestamp_t install_time, timestamp_t installing_txn_id, const constraint_fn &verify_fn)
      : install_time_(install_time), installing_txn_id_(installing_txn_id), verify_fn_(verify_fn) {}

  /**
   * Checks whether the passed in txn satisfies the constraint. Doesn't check constraint, if
   * constraint already violated. All calls to check constraint are expected to be atomic with respect to each other
   * @param txn the transaction verifying it passes the constraint
   * @return true if transaction can commit, false if it should abort
   */
  bool CheckConstraint(TransactionContext *txn);

  /**
   * Sets the constraint to be enforcing, any transaction that fails constraint check after this will be told to abort
   */
  void SetEnforcing() { enforcing_.store(true); }

  /**
   * Checks if the constraint is enforcing or not
   * @return whether the constraint is enforcing or not
   */
  bool IsEnforcing() { return enforcing_.load(); }

  /**
   * @return - whether constraint was violated or not
   */
  bool IsViolated() { return violated_.load(); }

  /**
   * Sets the violated flag of this constraint to true
   */
  void SetViolated() { violated_.store(true); }

  /**
   * Retrieve the installing transaction id
   * @return id of the transaction that installed this constraint
   */
  timestamp_t InstallingTransactionId() { return installing_txn_id_; }

  /**
   * Verifies if the constraint passes
   * @return true if constraint check passes, false if it fails
   */
  bool VerifyConstraint() { return verify_fn_(); }

 private:
  timestamp_t install_time_;            // The original start_time of the transaction installing constraint
  timestamp_t installing_txn_id_;       // The txn_id of the stinalling transaction
  const constraint_fn &verify_fn_;      // The function run to verify whether constraint is satisfied
  std::atomic<bool> violated_{false};   // Set to true if constraint is violated, then installing transaction aborts
  std::atomic<bool> enforcing_{false};  // Whether or not the constraint is enforcing
};
}  // namespace terrier::transaction
