#include "transaction/transaction_constraint.h"
#include "transaction/transaction_manager.h"

namespace terrier::transaction {

bool TransactionConstraint::CheckConstraint(TransactionContext * txn){
  //TODO(Yashwanth): add the parameters that need to be passed into verify_fn

  // Don't need to check the constraint if the transaction was started before install time, the installing
  // transaction would see it
  if(txn->StartTime() < install_time_){
    return true;
  }
  //if constraint already violated or pass check then can commit
  if(violated_.load() || verify_fn_()) {
    return true;
  } else{
    if(enforcing_.load()){ //if constraint enforcing and fail check then can't commit
      return false;
    } else {
      violated_.store(true); //if not enforcing and failed check, can commit but notify constraint to be violated
      return true;
    }
  }
}

}

