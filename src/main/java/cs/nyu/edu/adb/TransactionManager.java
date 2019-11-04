package cs.nyu.edu.adb;

import java.util.Arrays;
import java.util.List;

public class TransactionManager {

  public List<String> blockedTransactions;
  public List<Transaction> allTransactions;
  public List<Site> sites;
  public List<Operation> allOperations;

  public TransactionManager(List<Operation> allOperations) {
    // Initialize operations and sites
    this.allOperations = allOperations;
    sites = Arrays.asList(new Site[11]);
    for(int i = 1; i <= 20; ++i) {
      if(i % 2 == 1) {
        sites.get((1 + i) % 10).getDataManager().insertData(i, 10 * i);
      } else {
        for(int j = 1; j <= 10; ++j) {
          sites.get(j).getDataManager().insertData(i, 10 * i);
        }
      }
    }
  }

  public void run() {
    allOperations.forEach(operation -> {
      String op = operation.getName();
      switch (op) {
        case IOUtils.BEGIN:
          allTransactions.add(initTransaction(operation, false));
          break;
        case IOUtils.BEGIN_RO:
          allTransactions.add(initTransaction(operation, true));
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

  public Transaction initTransaction(Operation operation, boolean isReadOnly) {
    return new Transaction(operation.getName(), isReadOnly);
  }

  // TODO:
  public void commit() {

  }

  // TODO:
  public void fail() {

  }

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
