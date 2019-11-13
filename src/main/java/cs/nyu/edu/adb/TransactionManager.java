package cs.nyu.edu.adb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// TODO: Write comments for every method
public class TransactionManager {

//  public List<String> blockedTransactions;
  Integer currentTime;
  public List<Transaction> allTransactions;
  public List<Site> sites;
  public List<Operation> allOperations;
  public List<Integer> visitedTransactions;
  // variable -> waiting operations
  public Map<Integer, List<Operation>> waitingOperations;
  // waits for graph, transaction -> waiting transactions
  public Map<Integer, List<Integer>> waitsForGraph;
  // transactionID -> waiting sites
  public Map<Integer, List<Integer>> waitingSites;

  public TransactionManager(List<Operation> allOperations) {
    // Initialize operations and sites
    currentTime = 0;
    visitedTransactions = new ArrayList<>();
    allTransactions = new ArrayList<>();
//    blockedTransactions = new ArrayList<>();
    waitingOperations = new HashMap<>();
    waitsForGraph = new HashMap<>();
    waitingSites = new HashMap<>();
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
    currentTime = currentTime + 1;
    switch (op) {
      case IOUtils.BEGIN:
        allTransactions.add(initTransaction(operation, false, currentTime));
        break;
      case IOUtils.BEGIN_RO:
        allTransactions.add(initTransaction(operation, true, currentTime));
        break;
      case IOUtils.DUMP:
        dump();
        break;
      case IOUtils.END:
        Integer transactionID = Integer.valueOf(operation.getTransaction().substring(1));
        if(getTransaction(operation).getTransactionStatus()
            == TransactionStatus.SHOULD_BE_ABORT) {
          abort(transactionID);
        } else {
          commit(transactionID);
        }
        break;
      case IOUtils.FAIL:
        fail(operation.getSite());
        break;
      case IOUtils.READ:
        if(read(operation)) {
          System.out.println(String.format("Transaction %s can read variable %s",
              operation.getTransaction(), operation.getVariable().toString()));
        } else {
          System.out.println(String.format("Transaction %s cannot read variable %s",
              operation.getTransaction(), operation.getVariable().toString()));
        }
        break;
      case IOUtils.RECOVER:
        recover(operation.getSite());
        break;
      case IOUtils.WRITE:
        if(write(operation)) {
          System.out.println(String.format("Transaction %s can write variable %s to new value %s",
              operation.getTransaction(),
              operation.getVariable().toString(),
              operation.getWritesToValue().toString()));
        } else {
          System.out.println(String.format("Transaction %s cannot write variable %s to new value %s",
              operation.getTransaction(),
              operation.getVariable().toString(),
              operation.getWritesToValue().toString()));
        }
        break;
      default:
        throw new UnsupportedOperationException("This opetation is not being supported");
    }
  }

  public Transaction initTransaction(Operation operation, boolean isReadOnly, Integer timeStamp) {
    return new Transaction(operation.getTransaction(), isReadOnly, timeStamp);
  }

  public void abort(Integer transactionID) {
    commitOrAbort(transactionID, false);
  }

  public void commit(Integer transactionID) {
    commitOrAbort(transactionID, true);
  }

  private void commitOrAbort(Integer transactionID, boolean shouldCommit) {

    List<Integer> holdVariables = new ArrayList<>();
    // Iterate all sites and find variables which transaction holds lock on, and remove locks.
    for (int i = 1; i <= 10; ++i) {
      Site site = sites.get(i);

      // Add all variables holding by this transaction to a list
      site.getLockManager().readLocks.forEach((key, value) -> {
        if (value.contains(transactionID) && !holdVariables.contains(key)) {
          holdVariables.add(key);
        }
      });

      site.getLockManager().writeLock.forEach((key, value) -> {
        if(value.equals(transactionID) && !holdVariables.contains(key)) {
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
                    if (!holdVariables.contains(key)) {
                      holdVariables.add(key);
                    }
                    site.getDataManager().updateToCommittedValue(key);
                  }
                });
      }

      // Release all the locks
      site.getLockManager().readLocks.forEach((key, value) -> value.remove(transactionID));
      site.getLockManager()
          .writeLock.entrySet().removeIf(entry -> entry.getValue().equals(transactionID));
      site.getLockManager().readLocks.entrySet().removeIf(entry -> entry.getValue().size() == 0);
    }
    holdVariables.stream()
        .forEach(
            holdVariable -> {
              if (waitingOperations.containsKey(holdVariable)) {
                Operation waitingOperation = waitingOperations.get(holdVariable).get(0);
                boolean flag = true;
                while (waitingOperation != null && flag) {
                  if (waitingOperation.getName().equals(IOUtils.READ)) {
                    if (read(waitingOperation)) {
                      waitingOperation = addToWaitingOperations(holdVariable);
                    } else {
                      flag = false;
                    }
                  } else if (waitingOperation.getName().equals(IOUtils.WRITE)) {
                    if (write(waitingOperation)) {
                      waitingOperation = addToWaitingOperations(holdVariable);
                    } else {
                      flag = false;
                    }
                  } else {
                    throw new IllegalArgumentException("This operation should not be blocked");
                  }
                }
              }
            });
  }

  private Operation addToWaitingOperations(Integer holdVariable) {
    Operation waitingOperation;
    waitingOperations.get(holdVariable).remove(0);
    if (waitingOperations.get(holdVariable).size() == 0) {
      waitingOperations.remove(holdVariable);
    }
    if (!waitingOperations.containsKey(holdVariable)) {
      waitingOperation = null;
    } else {
      waitingOperation = waitingOperations.get(holdVariable).get(0);
    }
    return waitingOperation;
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
    DataManager dataManager = failSite.getDataManager();

    List<Integer> holdVariables = new ArrayList<>();

    lockManager.readLocks.forEach((key, value) -> {
      holdVariables.add(key);
      value.forEach(transaction ->
          getTransaction(transaction).setTransactionStatus(TransactionStatus.SHOULD_BE_ABORT));
    });

    lockManager.writeLock.forEach((key, value) -> {
      holdVariables.add(key);
      getTransaction(value).setTransactionStatus(TransactionStatus.SHOULD_BE_ABORT);
    });

    holdVariables.forEach(dataManager::updateToCommittedValue);

    // erase all the locks
    lockManager.readLocks.clear();
    lockManager.writeLock.clear();
  }


  public void recover(Integer site) {
    Site failSite = sites.get(site);
    failSite.isDown = false;
    for(Map.Entry<Integer, List<Integer>> entry : waitingSites.entrySet()) {
      List<Integer> sites = entry.getValue();
      if (sites.contains(site)) {
        sites.remove(site);
      }
    }
    waitingSites.forEach((key, value) -> {
      if(value.size() == 0) {
        Operation operation = getTransaction(key).getCurrentOperation();
        if(operation.getName().equals(IOUtils.READ)) {
          read(operation);
        } else if(operation.getName().equals(IOUtils.WRITE)) {
          write(operation);
        } else {
          throw new IllegalArgumentException("This operation should not be blocked");
        }
      }
    });
  }

  private boolean read(Operation operation) {

    Transaction transaction = getTransaction(operation);
    Integer var = operation.getVariable();
    Integer transactionID = Integer.valueOf(operation.getTransaction().substring(1));

    // set current operation
    transaction.setCurrentOperation(operation);

    if(transaction.isReadOnly()) {
      if(var % 2 == 1) {
        Site site = sites.get((1 + var) % 10);
        return readVariable(site, var, true, transaction);
      } else {
        return readVariable(sites.get(1), var, true, transaction);
      }

    } else {
      // If the variable is odd number
      if(var % 2 == 1) {
        Site site = sites.get((1 + var) % 10);
        boolean canRead = site.getLockManager().canRead(var, transactionID);
        if(site.isDown || !canRead) {
          if(!site.isDown) {
            blockReadTransaction(site, var, operation, transactionID);
            detectDeadLock(transaction);
          } else {
            addToWaitingSiteList(transactionID, (1 + var) % 10);
          }
//          blockTransaction(transaction);
          return false;
        } else {
          return readVariable(site, var, false, transaction);
        }
      } else {
        for(int i = 1; i <= 10; ++i) {
          if(!sites.get(i).isDown && sites.get(i).getLockManager()
              .canRead(var, transactionID)) {
            return readVariable(sites.get(i), var, false, transaction);
          }
        }
        for (int i = 1; i <= 10; i++) {
          if(!sites.get(i).isDown) {
            blockReadTransaction(sites.get(i), var, operation, transactionID);
            detectDeadLock(transaction);
          } else {
            addToWaitingSiteList(transactionID, i);
          }
        }
//        blockTransaction(transaction);
        return false;
      }
    }
  }

  private void addToWaitingSiteList(Integer transactionID, Integer site) {
    if(waitingSites.containsKey(transactionID)) {
      waitingSites.get(transactionID).add(site);
    } else {
      waitingSites.put(transactionID, new ArrayList<>());
      waitingSites.get(transactionID).add(site);
    }
  }

  private boolean readVariable(Site site,
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

    // set current operation
    transaction.setCurrentOperation(operation);

    // if it's odd number
    if(var % 2 == 1) {
      Site site = sites.get((1 + var) % 10);
      if(site.isDown || !site.getLockManager().canWrite(var, transactionID)) {
        if(!site.isDown) {
          blockWriteTransaction(site, var, operation, transactionID);
          detectDeadLock(transaction);
        } else {
          addToWaitingSiteList(transactionID, (1 + var) % 10);
        }
//        blockTransaction(transaction);
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
          if(!site.isDown) {
            blockWriteTransaction(site, var, operation, transactionID);
            detectDeadLock(transaction);
          } else {
            addToWaitingSiteList(transactionID, i);
          }
          flag = false;
        }
      }
      if(!flag) {
//        blockTransaction(transaction);
        return false;
      } else {
        sites.stream().skip(1).forEach(site -> {
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
        waitingOperations.put(var, new ArrayList<>());
        waitingOperations.get(var).add(operation);
      }
      if (site.getLockManager().readLocks.containsKey(var)) {
        site.getLockManager().readLocks.get(var).stream()
            .forEach(
                t -> {
                  if (t != transactionID) {
                    if (waitsForGraph.containsKey(transactionID)) {
                      waitsForGraph.get(transactionID).add(t);
                    } else {
                      waitsForGraph.put(transactionID, new ArrayList<>());
                      waitsForGraph.get(transactionID).add(t);
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
        if (!waitingOperations.get(var).contains(operation)) {
          waitingOperations.get(var).add(operation);
        }
      } else {
        waitingOperations.put(var, new ArrayList<>());
        waitingOperations.get(var).add(operation);
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
          waitsForGraph.put(transactionID, new ArrayList<>());
          waitsForGraph.get(transactionID).add(t);
        }
      }
    }
  }

  /*
  private void blockTransaction(Transaction transaction) {
    blockedTransactions.add(transaction.getName());
  }
   */

  public void dump() {
    for (int i = 1; i <= 10; i++) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("site " + i + " - ");
      Map<Integer, Integer> sortedVariables = sites.get(i)
          .getDataManager()
          .getAllSortedCommittedValues();
      sortedVariables.forEach((key, value) ->
          stringBuilder.append("x" + key + ": " + value + ", "));
      System.out.println(stringBuilder);
    }
  }

  private void detectDeadLock(Transaction transaction) {
    Integer transactionID = Integer.valueOf(transaction.getName().substring(1));
    if(containsDeadLock()) {
      // Find the youngest transaction
      Integer youngestTransaction = transactionID;
      Integer earliestTime = Integer.MAX_VALUE;
      for(Integer visitedTransaction : visitedTransactions) {
        if(getTransaction(visitedTransaction).getTimeStamp() < earliestTime) {
          earliestTime = getTransaction(visitedTransaction).getTimeStamp();
          youngestTransaction = visitedTransaction;
        }
      }
      abort(youngestTransaction);
      transaction.setTransactionStatus(TransactionStatus.IS_FINISHED);
    }
  }

  private boolean containsDeadLock() {
    for(Transaction transaction : allTransactions) {
      Integer transactionID = Integer.valueOf(transaction.getName().substring(1));
      visitedTransactions.clear();
      if(containsCircle(transactionID)) {
        return true;
      }
    }
    return false;
  }

  private boolean containsCircle(Integer transactionID) {
    if(visitedTransactions.contains(transactionID)) {
      return true;
    } else {
       visitedTransactions.add(transactionID);
    }
    if(!waitsForGraph.containsKey(transactionID)) {
      return false;
    }
    for (int i = 0; i < waitsForGraph.get(transactionID).size(); i++) {
      if(containsCircle(waitsForGraph.get(transactionID).get(i))) {
        return true;
      }
    }
    visitedTransactions.remove(transactionID);
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
        .filter(transaction -> transaction.getName().equals(
            new StringBuilder("T" + transactionIndex).toString()))
        .collect(Collectors.toList());
    return transactions.get(0);
  }

}
