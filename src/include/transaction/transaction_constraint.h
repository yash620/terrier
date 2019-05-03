#pragma once

#include "transaction/transaction_defs.h"
#include "transaction/transaction_context.h"
/**
 * A constraint that needs to be checked before a transaction can commit
 */
namespace terrier::transaction {
class TransactionConstraint{
  public:
    /**
     * Initializes a new TransactionConstraint.
     * @param install_time The original start_time of the transaction installing constraint
     * @param verify_fn The function run to verify whether constraint is satisfied, returns true if constraint satisfied
     */
    TransactionConstraint(const timestamp_t install_time, const constraint_fn verify_fn)
        : install_time_(install_time), verify_fn_ (verify_fn) {}

    /**
     * Checks whether the passed in txn satisfies the constraint. Doesn't check constraint, if
     * constraint already violated
     * @param txn the transaction verifying it passes the constraint
     * @return true if transaction can commit, false if it should abort
     */
    bool CheckConstraint(TransactionContext * txn);

    /**
     * Sets the constraint to be enforcing, any transaction that fails constraint check after this will be told to abort
     */
    void SetEnforcing(){
      enforcing = true;
    }

    /**
     * @return - whether constraint was violated or not
     */
    bool Violated(){
      return violated;
    }


  private:
    timestamp_t install_time_; // The original start_time of the transaction installing constraint
    constraint_fn verify_fn_; // The function run to verify whether constraint is satisfied
    bool violated; // Set to true if constraint is violated, then installing transaction aborts
    bool enforcing; // Whether or not the constraint is enforcing

};
}