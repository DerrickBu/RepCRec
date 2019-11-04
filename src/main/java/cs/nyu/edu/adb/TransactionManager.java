package cs.nyu.edu.adb;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
        sites.get((1 + i) % 10).getDataManager().insertValue(i, 10 * i);
      } else {
        for(int j = 1; j <= 10; ++j) {
          sites.get(j).getDataManager().insertValue(i, 10 * i);
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
          fail(operation.getSite());
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
    return new Transaction(operation.getTransaction(), isReadOnly);
  }

  // TODO: Commit values, release read and write locks, execute operations being blocked by the locks
  public void commit() {

  }

  public void fail(Integer site) {
    Site failSite = sites.get(site);
    failSite.isDown = true;

    // Mark all transactions which have accessed items in this site 'SHOULD_BE_ABORTED'
    LockManager lockManager = failSite.getLockManager();

    lockManager.readLocks.entrySet().stream().forEach(entry ->
      entry.getValue().stream().forEach(transaction ->
          getTransaction(transaction).setTransactionStatus(TransactionStatus.SHOULD_BE_ABORT)));

    lockManager.writeLock.entrySet().stream().forEach(entry ->
            getTransaction(entry.getValue()).setTransactionStatus(TransactionStatus.SHOULD_BE_ABORT));
  }

  // TODO:
  public void abort() {

  }

  private boolean read(Operation operation) {

    Transaction transaction = getTransaction(operation);
    Integer var = operation.getVariable();

    // if the transaction has already been blocked
    if(transaction.getTransactionStatus() == TransactionStatus.IS_BLOCKED) {
      transaction.waitingOperatons.add(operation);
      return false;
    }

    // set current operation
    transaction.setCurrentOperation(operation);

    // TODO: support read only transaction read
    if(transaction.isReadOnly()) {
      if(var % 2 == 1) {
        Site site = sites.get((1 + var) % 10);
        return readVar(site, var, true);
      } else {
        // TODO: Not sure about the logic here
        return readVar(sites.get(0), var, true);
      }

    } else {
      // If the variable is odd number
      if(var % 2 == 1) {
        Site site = sites.get((1 + var) % 10);
        boolean canRead = site.getLockManager().canRead(var,
            Integer.valueOf(operation.getTransaction().substring(1)));
        if(site.isDown || !canRead) {
          blockTransaction(transaction);
          return false;
        } else {
          return readVar(site, var, false);
        }
      } else {
        for(int i = 1; i <= 10; ++i) {
          if(!sites.get(i).isDown && sites.get(i).getLockManager().canRead(var,
              Integer.valueOf(operation.getTransaction().substring(1)))) {
            return readVar(sites.get(i), var, false);
          }
        }
        blockTransaction(transaction);
        return false;
      }
    }
  }

  private boolean readVar(Site site, Integer var, boolean readCommittedValue) {
    Integer val = readCommittedValue ? site.getDataManager().getCommittedValue(var) :
        site.getDataManager().getCurValue(var);
    System.out.println("x" + var + ": " + val);
    return true;
  }

  private boolean write(Operation operation) {
    Transaction transaction = getTransaction(operation);
    Integer value = operation.getWritesToValue();
    Integer var = operation.getVariable();
    Integer transactionID = Integer.valueOf(operation.getTransaction().substring(1));

    // if the transaction has already been blocked
    if(transaction.getTransactionStatus() == TransactionStatus.IS_BLOCKED) {
      transaction.waitingOperatons.add(operation);
      return false;
    }

    // set current operation
    transaction.setCurrentOperation(operation);

    // if it's odd number
    if(var % 2 == 1) {
      Site site = sites.get((1 + var) % 10);
      if(site.isDown || !site.getLockManager().canWrite(var, transactionID)) {
        blockTransaction(transaction);
        return false;
      } else {
        site.getDataManager().updateValue(var, value);
        return true;
      }
    } else {
      List<Site> activeSites = sites.stream()
          .filter(site -> !site.isDown && site.getLockManager().canWrite(var, transactionID))
          .collect(Collectors.toList());
      if(activeSites.size() != sites.size()) {
        blockTransaction(transaction);
        return false;
      } else {
        sites.stream().forEach(site -> site.getDataManager().updateValue(var, value));
        return true;
      }
    }
  }

  private void blockTransaction(Transaction transaction) {
    transaction.setTransactionStatus(TransactionStatus.IS_BLOCKED);
    blockedTransactions.add(transaction.getName());
  }

  // TODO: Print all the variables and values in each site
  public void dump() {

  }

  // TODO:
  public void recover() {

  }

  // TODO:
  public boolean detectDeadLock() {
    return false;
  }

  private Transaction getTransaction(Operation operation) {
    List<Transaction> transactions = allTransactions.stream()
        .filter(transaction -> transaction.getName().equals(operation.getTransaction()))
        .collect(Collectors.toList());
    return transactions.get(0);
  }

  private Transaction getTransaction(Integer transactionIndex) {
    List<Transaction> transactions = allTransactions.stream()
        .filter(transaction -> transaction.getName().equals(new StringBuilder("T" + transactionIndex)))
        .collect(Collectors.toList());
    return transactions.get(0);
  }

}
