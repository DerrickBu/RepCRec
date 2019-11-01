package cs.nyu.edu.adb;

import java.util.List;

public class TransactionManager {

  List<String> blockedTransactions;
  List<String> allTransactions;
  List<Site> sites;
  List<Operation> allOperations;

  public TransactionManager(List<Operation> allOperations) {
    // Initialize operations and sites
    this.allOperations = allOperations;

  }

  public void run() {
    allOperations.forEach(operation -> {
      String op = operation.getName();
      switch (op) {
        case IOUtils.BEGIN:
          break;
        case IOUtils.BEGIN_RO:
          break;
        case IOUtils.DUMP:
          break;
        case IOUtils.END:
          break;
        case IOUtils.FAIL:
          break;
        case IOUtils.READ:
          break;
        case IOUtils.RECOVER:
          break;
        case IOUtils.WRITE:
          break;
          default:

      }
    });
  }

  // TODO:
  public void commit() {

  }

  // TODO:
  public void fail() {

  }

  // TODO:
  public void read() {

  }

  // TODO:
  public void write() {

  }

  // TODO:
  public void dump() {

  }

  // TODO:
  public void recover() {

  }

  // TODO:
  public boolean detectDeadLock() {
    return false;
  }

}
