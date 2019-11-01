package cs.nyu.edu.adb;

import java.util.List;

public class Transaction {

  public String name;
  Operation currentOperation;
  TransactionStatus transactionStatus;

  public Transaction() {
    transactionStatus = TransactionStatus.ACTIVE;
  }

}
