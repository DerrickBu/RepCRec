package cs.nyu.edu.adb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransactionManager {

  public List<String> blockedTransactions;
  public List<Transaction> allTransactions;
  public List<Site> sites;
  public List<Operation> allOperations;
  // variable -> waiting operations
  public Map<Integer, List<Operation>> waitingOperations;
  // waits for graph
  public Map<Integer, List<Integer>> waitsForGraph;

  public TransactionManager(List<Operation> allOperations) {
    // Initialize operations and sites
    allTransactions = new ArrayList<>();
    waitingOperations = new HashMap<>();
    waitsForGraph = new HashMap<>();
    this.allOperations = allOperations;
    sites = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      sites.add(new Site());
    }
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
    allOperations.forEach(operation -> executeOperation(operation));
  }

  public void executeOperation(Operation operation) {
    String op = operation.getName();
    switch (op) {
      case IOUtils.BEGIN:
        allTransactions.add(initTransaction(operation, false));
        break;
      case IOUtils.BEGIN_RO:
        allTransactions.add(initTransaction(operation, true));
        break;
      case IOUtils.DUMP:
        dump();
        break;
      case IOUtils.END:
        if(getTransaction(operation).getTransactionStatus()
            == TransactionStatus.SHOULD_BE_ABORT) {
          abort(operation);
        } else {
          commit(operation);
        }
        break;
      case IOUtils.FAIL:
        fail(operation.getSite());
        break;
      case IOUtils.READ:
        if(read(operation)) {
          System.out.println(String.format("Transaction {} can read variable {}",
              operation.getTransaction(), operation.getVariable()));
        } else {
          System.out.println(String.format("Transaction {} can not read variable {}",
              operation.getTransaction(), operation.getVariable()));
        }
        break;
      case IOUtils.RECOVER:
        recover(operation.getSite());
        break;
      case IOUtils.WRITE:
        if(write(operation)) {
          System.out.println(String.format("Transaction {} can write variable {} to new value {}",
              operation.getTransaction(), operation.getVariable(), operation.getWritesToValue()));
        } else {
          System.out.println(String.format("Transaction {} can not write variable {} to new value {}",
              operation.getTransaction(), operation.getVariable(), operation.getWritesToValue()));
        }
        break;
      default:
        throw new UnsupportedOperationException("This opetation is not being supported");
    }
  }

  public Transaction initTransaction(Operation operation, boolean isReadOnly) {
    return new Transaction(operation.getTransaction(), isReadOnly);
  }

  public void abort(Operation operation) {
    commitOrAbort(operation, false);
  }

  public void commit(Operation operation) {
    commitOrAbort(operation, true);
  }

  private void commitOrAbort(Operation operation, boolean shouldCommit) {
    Integer transactionID = Integer.valueOf(operation.getTransaction().substring(1));

    List<Integer> holdVariables = new ArrayList<>();
    // Iterate all sites and find variables which transaction holds lock on, and remove locks.
    for (int i = 1; i <= 10; ++i) {
      Site site = sites.get(i);

      // Add all variables holding by this transaction to a list
      site.getLockManager().readLocks.forEach((key, value) -> {
        if(value.contains(transactionID)) {
          holdVariables.add(key);
        }
      });

      if (shouldCommit) {
        // Update committed to current values
        site.getLockManager()
            .writeLock
            .forEach(
                (key, value) -> {
                  if (value == transactionID) {
                    holdVariables.add(key);
                    site.getDataManager().updateToCurValue(key);
                  }
                });
      } else {
        // Revert current value to committed value
        site.getLockManager()
            .writeLock
            .forEach(
                (key, value) -> {
                  if (value == transactionID) {
                    holdVariables.add(key);
                    site.getDataManager().updateToCommittedValue(key);
                  }
                });
      }

      // Release all the locks
      site.getLockManager().readLocks.forEach((key, value) -> value.remove(transactionID));
      site.getLockManager()
          .writeLock.entrySet().removeIf(entry -> entry.getValue().equals(transactionID));
    }
    holdVariables.stream().forEach(holdVariable -> {
      if(waitingOperations.containsKey(holdVariable)) {
        if(operation.getName().equals(IOUtils.READ)) {
          while(read(waitingOperations.get(holdVariable).get(0))) {
            waitingOperations.get(holdVariable).remove(0);
          }
        } else if(operation.getName().equals(IOUtils.WRITE)) {
          while(write(waitingOperations.get(holdVariable).get(0))) {
            waitingOperations.get(holdVariable).remove(0);
          }
        } else {
          throw new IllegalArgumentException("This operation should not be blocked");
        }
      }
    });
  }

  /**
   * Fail a site
   * Mark the given site to be down
   * Mark all the transactions holding lock to be 'SHOULD_BE_ABORTED'
   * @param site given to fail
   */
  public void fail(Integer site) {
    Site failSite = sites.get(site);
    failSite.isDown = true;

    // Mark all transactions which have accessed items in this site 'SHOULD_BE_ABORTED'
    LockManager lockManager = failSite.getLockManager();

    lockManager.readLocks.forEach((key, value) ->
      value.forEach(transaction ->
          getTransaction(transaction).setTransactionStatus(TransactionStatus.SHOULD_BE_ABORT)));

    lockManager.writeLock.forEach((key, value) ->
            getTransaction(value).setTransactionStatus(TransactionStatus.SHOULD_BE_ABORT));

    // erase all the locks
    lockManager.readLocks.clear();
    lockManager.writeLock.clear();
  }


  public void recover(Integer site) {
    Site failSite = sites.get(site);
    failSite.isDown = false;

  }

  private boolean read(Operation operation) {

    Transaction transaction = getTransaction(operation);
    Integer var = operation.getVariable();
    Integer transactionID = Integer.valueOf(operation.getTransaction().substring(1));

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
        return readVar(site, var, true, transaction);
      } else {
        // TODO: Not sure about the logic here
        return readVar(sites.get(0), var, true, transaction);
      }

    } else {
      // If the variable is odd number
      if(var % 2 == 1) {
        Site site = sites.get((1 + var) % 10);
        boolean canRead = site.getLockManager().canRead(var, transactionID);
        if(site.isDown || !canRead) {
          if(!site.isDown) {
            blockReadTransaction(site, var, operation, transactionID);
          }
          blockTransaction(transaction);
          return false;
        } else {
          return readVar(site, var, false, transaction);
        }
      } else {
        for(int i = 1; i <= 10; ++i) {
          if(!sites.get(i).isDown && sites.get(i).getLockManager()
              .canRead(var, transactionID)) {
            return readVar(sites.get(i), var, false, transaction);
          }
        }
        for (int i = 1; i <= 10; i++) {
          if(!sites.get(i).isDown) {
            blockReadTransaction(sites.get(i), var, operation, transactionID);
          }
        }
        blockTransaction(transaction);
        return false;
      }
    }
  }

  private boolean readVar(Site site,
      Integer var,
      boolean readCommittedValue,
      Transaction transaction) {
    Integer val = readCommittedValue ? site.getDataManager().getCommittedValue(var) :
        site.getDataManager().getCurValue(var);
    if (transaction.getTransactionStatus() == TransactionStatus.ACTIVE) {
      System.out.println("x" + var + ": " + val);
    }
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
        blockWriteTransaction(site, var, operation, transactionID);
        blockTransaction(transaction);
        return false;
      } else {
        site.getLockManager().write(var, transactionID);
        site.getDataManager().updateValue(var, value);
        return true;
      }
    } else {
      boolean flag = true;
      for (int i = 1; i <= 10; i++) {
        Site site = sites.get(i);
        if(site.isDown || !site.getLockManager().canWrite(var, transactionID)) {
          blockWriteTransaction(site, var, operation, transactionID);
          flag = false;
        }
      }
      if(!flag) {
        blockTransaction(transaction);
        return false;
      } else {
        sites.stream().forEach(site -> {
          site.getLockManager().write(var, transactionID);
          site.getDataManager().updateValue(var, value);
        });
        return true;
      }
    }
  }

  private void blockWriteTransaction(
      Site site,
      Integer var,
      Operation operation,
      Integer transactionID) {
    if (!site.isDown) {
      if (waitingOperations.containsKey(var)) {
        waitingOperations.get(var).add(operation);
      } else {
        waitingOperations.put(var, Arrays.asList(operation));
      }
      if (site.getLockManager().readLocks.containsKey(var)) {
        site.getLockManager().readLocks.get(var).stream()
            .forEach(
                t -> {
                  if (t != transactionID) {
                    if (waitsForGraph.containsKey(transactionID)) {
                      waitsForGraph.get(transactionID).add(t);
                    } else {
                      waitsForGraph.put(transactionID, Arrays.asList(t));
                    }
                  }
                });
      }
      checkWriteLocks(site, var, transactionID);
    }
  }

  private void blockReadTransaction(
      Site site,
      Integer var,
      Operation operation,
      Integer transactionID) {
    if (!site.isDown) {
      if (waitingOperations.containsKey(var)) {
        waitingOperations.get(var).add(operation);
      } else {
        waitingOperations.put(var, Arrays.asList(operation));
      }
      checkWriteLocks(site, var, transactionID);
    }
  }

  private void checkWriteLocks(Site site,
      Integer var,
      Integer transactionID) {
    if (site.getLockManager().writeLock.containsKey(var)) {
      Integer t = site.getLockManager().writeLock.get(var);
      if (t != transactionID) {
        if (waitsForGraph.containsKey(transactionID)) {
          waitsForGraph.get(transactionID).add(t);
        } else {
          waitsForGraph.put(transactionID, Arrays.asList(t));
        }
      }
    }
  }

  private void blockTransaction(Transaction transaction) {
    transaction.setTransactionStatus(TransactionStatus.IS_BLOCKED);
    blockedTransactions.add(transaction.getName());
  }


  // TODO: Print all the variables and values in each site
  public void dump() {
    for (int i = 1; i <= 10; i++) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("site " + i + " - ");
      Map<Integer, Integer> sortedVariables = sites.get(i)
          .getDataManager()
          .getAllSortedCommittedValues();
      sortedVariables.forEach((key, value) ->
          stringBuilder.append(key + ": " + value + ", "));
      System.out.println(stringBuilder);
    }
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
