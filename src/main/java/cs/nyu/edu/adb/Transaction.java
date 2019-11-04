package cs.nyu.edu.adb;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

  private String name;
  private Operation currentOperation;
  private TransactionStatus transactionStatus;
  private boolean isReadOnly;
  public List<Operation> waitingOperatons;

  public Transaction(String name, boolean isReadOnly) {
    transactionStatus = TransactionStatus.ACTIVE;
    this.name = name;
    this.isReadOnly = isReadOnly;
    waitingOperatons = new ArrayList<>();
  }

  public boolean isReadOnly() {
    return isReadOnly;
  }

  public String getName() {
    return name;
  }

  public TransactionStatus getTransactionStatus() {
    return transactionStatus;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setTransactionStatus(TransactionStatus transactionStatus) {
    this.transactionStatus = transactionStatus;
  }

  public Operation getCurrentOperation() {
    return currentOperation;
  }

  public void setCurrentOperation(Operation currentOperation) {
    this.currentOperation = currentOperation;
  }
}
