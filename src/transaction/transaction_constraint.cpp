#include "transaction/transaction_constraint.h"

namespace terrier::transaction {

bool TransactionConstraint::CheckConstraint(TransactionContext * txn){
  //TODO(Yashwanth): add the parameters that need to be passed into verify_fn

  // Don't need to check the constraint if the transaction was started before install time, the installing
  // transaction would see it
  if(txn->StartTime() < install_time_){
    return true;
  }

  if(violated || verify_fn_()) {
    return true;
  } else{
    if(enforcing){
      return false;
    } else {
      violated = true;
      return true;
    }
  }
}

}

