package cs.nyu.edu.adb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransactionManager {

  private Integer currentTime;
  private List<Transaction> allTransactions;
  private List<Site> sites;
  private List<Operation> allOperations;
  private List<Integer> visitedTransactions;

  // variable -> waiting operations
  private Map<Integer, List<Operation>> waitingOperations;

  // waits for graph, transaction -> waiting transactions
  private Map<Integer, List<Integer>> waitsForGraph;

  // transactionID -> waiting sites
  private Map<Integer, List<Integer>> waitingSites;

  /**
   * Here we initialize operations and sites.
   * There are ten sites which indexes are range from 1 - 10
   * We also initialize LockManager and DataManager for a new site
   * Node that variable with even index populate in all sites, and
   * variable with odd index exists in site[(1 + index) % 10]
   * The initial value for variables is var_index * 10;
   * @param allOperations parsed operations by IOUtils from input file
   */
  public TransactionManager(List<Operation> allOperations) {
    currentTime = 0;
    visitedTransactions = new ArrayList<>();
    allTransactions = new ArrayList<>();
    waitingOperations = new HashMap<>();
    waitsForGraph = new HashMap<>();
    waitingSites = new HashMap<>();
    this.allOperations = allOperations;
    sites = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      sites.add(new Site(i));
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

  /**
   * Method to start TransactionManager and execute all operations
   */
  public void run() {
    allOperations.forEach(this::executeOperation);
  }

  /**
   * Execute one operation.
   * There are 8 types of operations in total:
   * begin, beginRO, dump, end, fail, recover, read and write.
   * Will throw 'UnsupportedOperationException' if we get an operation whose type is unknown.
   * We will commit, abort or do nothing when we meet 'end' operation
   * Doing nothing is because we have aborted this transaction due to deadlock before.
   * @param operation given to execute
   */
  private void executeOperation(Operation operation) {
    String op = operation.getName();
    currentTime = currentTime + 1;
    Integer transactionID;
    switch (op) {
      case IOUtils.BEGIN:
        IOUtils.beginOutputMessage(operation);
        allTransactions.add(initTransaction(operation, false, currentTime));
        break;
      case IOUtils.BEGIN_RO:
        IOUtils.beginROOutputMessage(operation);
        allTransactions.add(initTransaction(operation, true, currentTime));
        break;
      case IOUtils.DUMP:
        dump();
        break;
      case IOUtils.END:
        transactionID = Integer.valueOf(operation.getTransaction().substring(1));
        if(getTransaction(operation).getTransactionStatus()
            == TransactionStatus.SHOULD_BE_ABORT) {
          abort(transactionID);
        } else if(getTransaction(operation).getTransactionStatus()
            == TransactionStatus.ACTIVE){
          commit(transactionID);
        }
        break;
      case IOUtils.FAIL:
        IOUtils.failOutputMessage(operation);
        fail(operation.getSite());
        break;
      case IOUtils.READ:
        if(!read(operation)) {
          IOUtils.cannotReadOutputMessage(operation);
        }
        break;
      case IOUtils.RECOVER:
        IOUtils.recoverOutputMessage(operation);
        recover(operation.getSite());
        break;
      case IOUtils.WRITE:
        transactionID = Integer.valueOf(operation.getTransaction().substring(1));
        if(write(operation)) {
          IOUtils.canWriteOutputMessage(operation);
        } else {
          // Check prevent print message bug because of deadlock
          Integer variable = operation.getVariable();
          if (waitingOperations.containsKey(variable)
              && waitingOperations.get(variable).contains(operation)
              || waitingSites.containsKey(transactionID)) {
            IOUtils.cannotWriteOutputMessage(operation);
          }
        }
        break;
      default:
        throw new UnsupportedOperationException("This operation is not being supported");
    }
  }

  /**
   * Check if there's write operation waiting for the same variable
   * @param operation given to get variable
   * @return true if there's write operation waiting for the given variable, false if there's not
   */
  private boolean hashWriteWaiting(Operation operation) {
    Integer variable = operation.getVariable();
    return (waitingOperations.containsKey(variable)
        && !waitingOperations.get(variable).contains(operation)
        && containsWriteOperation(waitingOperations.get(variable)));
  }

  /**
   * Check if there's write operation in the waiting list
   * @param operations given to check if there's waiting operation
   * @return true if there's waiting write operation in the list, false if there's not
   */
  private boolean containsWriteOperation(List<Operation> operations) {
    operations.stream().filter(operation -> operation.getName().equals(IOUtils.WRITE));
    return operations.size() > 0;
  }

  /**
   * Create Transaction object when a new transaction begins
   * @param operation given to get the transaction name
   * @param isReadOnly If this transaction is read only
   * @param timeStamp shows the time when we create this transaction
   * @return a new Transaction object
   */
  private Transaction initTransaction(Operation operation, boolean isReadOnly, Integer timeStamp) {
    return new Transaction(operation.getTransaction(), isReadOnly, timeStamp);
  }

  /**
   * Abort a transaction
   * @param transactionID given to abort
   */
  private void abort(Integer transactionID) {
    IOUtils.abortOutputMessage(transactionID);
    commitOrAbort(transactionID, false);
  }

  /**
   * Commit a transaction
   * @param transactionID given to commit
   */
  private void commit(Integer transactionID) {
    IOUtils.commitOutputMessage(transactionID);
    commitOrAbort(transactionID, true);
  }

  /**
   * Fail a site
   * Mark the given site to be down
   * Mark all the transactions holding lock to be 'SHOULD_BE_ABORTED'
   * Clear the lock table
   * Revert all the variables' current values to committed values
   * @param site given to fail
   */
  private void fail(Integer site) {
    Site failSite = sites.get(site);
    failSite.setIsDown(true);

    // Mark all transactions which have accessed items in this site 'SHOULD_BE_ABORTED'
    LockManager lockManager = failSite.getLockManager();
    DataManager dataManager = failSite.getDataManager();

    List<Integer> holdVariables = new ArrayList<>();

    lockManager.getReadLocks().forEach((key, value) -> {
      holdVariables.add(key);
      value.forEach(transaction ->
          getTransaction(transaction).setTransactionStatus(TransactionStatus.SHOULD_BE_ABORT));
    });

    lockManager.getWriteLock().forEach((key, value) -> {
      holdVariables.add(key);
      getTransaction(value).setTransactionStatus(TransactionStatus.SHOULD_BE_ABORT);
    });

    holdVariables.forEach(dataManager::updateToCommittedValue);

    // erase all the locks
    lockManager.getReadLocks().clear();
    lockManager.getWriteLock().clear();
  }

  /**
   * Recover a site
   * Wake up operations which are being blocked since this site is down
   * @param site index given to recover
   */
  private void recover(Integer site) {
    Site failSite = sites.get(site);
    failSite.setIsDown(false);

    // erase site from waiting sites lists
    for(Map.Entry<Integer, List<Integer>> entry : waitingSites.entrySet()) {
      List<Integer> sites = entry.getValue();
      sites.remove(site);
    }

    // If a list is empty after removal, it means this operation is waiting for nothing
    // and could be waked up
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

  /**
   * Execute read operation, read variable from available sites
   * If one site is down, then we cannot read from it
   * If there is lock conflict in one site, then we also cannot read from it
   * Get value if we can read
   * Put operation to the waiting list if there's no available sites to read from
   * Detect if there's deadlock after blocking this operation.
   * Abort the youngest transaction if there's deadlock right now
   * @param operation given to run read operation
   * @return true if we there exists any site to read from, false if all sites are unavailable
   */
  private boolean read(Operation operation) {

    Transaction transaction = getTransaction(operation);
    Integer variable = operation.getVariable();

    // set current operation
    transaction.setCurrentOperation(operation);

    if(transaction.isReadOnly()) {
      readVariableForReadOnly(variable, transaction);
      return true;
    } else {
      // If the variable is odd number
      if(variable % 2 == 1) {
        return readVariableWithOddIndex(variable, transaction, operation);
      } else {
        return readVariableWithEvenIndex(variable, transaction, operation);
      }
    }
  }

  /**
   * Read a variable for read only transaction
   * @param variable given to read
   * @param transaction given to get transaction status
   */
  private void readVariableForReadOnly(Integer variable, Transaction transaction) {
    if(variable % 2 == 1) {
      Site site = sites.get((1 + variable) % 10);
      readVariable(site, variable, true, transaction);
    } else {
      readVariable(sites.get(1), variable, true, transaction);
    }
  }

  /**
   * Read value of a variable with even index
   * @param variable given to read
   * @param transaction given to get status and build waits for graph if cannot read
   * @param operation given to block if we cannot read
   * @return true if can read, false if cannot read
   */
  private boolean readVariableWithEvenIndex(
      Integer variable,
      Transaction transaction,
      Operation operation) {
    Integer transactionID = Integer.valueOf(operation.getTransaction().substring(1));
    for(int i = 1; i <= 10; ++i) {
      Site site = sites.get(i);
      if(!site.isDown() && site.getLockManager()
          .canRead(variable, transactionID)) {
        return canReadPreventWriteStarvation(site, variable, operation, transactionID, transaction);
      }
    }
    sites.stream().skip(1).forEach(site ->
        blockOperation(site, variable, operation, transactionID, true));
    return false;
  }

  /**
   * Read value of a variable with odd index
   * @param variable given to read
   * @param transaction given to get status and build waits for graph if cannot read
   * @param operation given to block if we cannot read
   * @return true if can read, false if cannot read
   */
  private boolean readVariableWithOddIndex(
      Integer variable,
      Transaction transaction,
      Operation operation) {
    Site site = sites.get((1 + variable) % 10);
    Integer transactionID = Integer.valueOf(operation.getTransaction().substring(1));
    boolean canRead = site.getLockManager().canRead(variable, transactionID);
    if(site.isDown() || !canRead) {
      blockOperation(site, variable, operation, transactionID, true);
      return false;
    } else {
      return canReadPreventWriteStarvation(site, variable, operation, transactionID, transaction);
    }
  }

  /**
   * Check if we could read variable with consideration of write starvation
   * @param site given to check lock table
   * @param variable used to block operation
   * @param operation used to be blocked if there's write starvation
   * @param transactionID used to build wait for graph
   * @param transaction used to read variable if there's not write starvation
   * @return true if we could read a variable
   * false if there's write starvation and we need to block that operation
   */
  private boolean canReadPreventWriteStarvation(
      Site site,
      Integer variable,
      Operation operation,
      Integer transactionID,
      Transaction transaction) {
    if(!site.getLockManager().getWriteLock().containsKey(variable)
        && hashWriteWaiting(operation)) {
      putToWaitingOperations(variable, operation);
      checkReadLocks(site, variable, transactionID);
      return false;
    }
    site.getLockManager().addReadLock(variable, transactionID);
    readVariable(site, variable, false, transaction);
    return true;
  }

  /**
   * Execute write operation
   * Write the new value only if all the sites which store the corresponding variable is available
   * Block the operation if there's at least one site available
   * Check if there's deadlock if we block this operation, and abort the youngest transaction
   * if there's deadlock circle
   * @param operation given to write new value
   * @return true if we could write, false if we cannot
   */
  private boolean write(Operation operation) {
    Transaction transaction = getTransaction(operation);
    Integer value = operation.getWritesToValue();
    Integer variable = operation.getVariable();
    Integer transactionID = Integer.valueOf(operation.getTransaction().substring(1));

    // set current operation
    transaction.setCurrentOperation(operation);

    // if it's odd number
    if(variable % 2 == 1) {
      Site site = sites.get((1 + variable) % 10);
      if(site.isDown() || !site.getLockManager().canWrite(variable, transactionID)) {
        blockOperation(site, variable, operation, transactionID, false);
        return false;
      } else {
        site.getLockManager().addWriteLock(variable, transactionID);
        site.getDataManager().updateValue(variable, value);
        return true;
      }
    } else {
      if(!canWrite(variable, transactionID, operation)) {
        return false;
      } else {
        sites.stream().skip(1).forEach(site -> {
          site.getLockManager().addWriteLock(variable, transactionID);
          site.getDataManager().updateValue(variable, value);
        });
        return true;
      }
    }
  }

  /**
   * Check if we could write variable with even index to new value
   * @param variable given to write
   * @param transactionID given to check locks
   * @param operation given to block if cannot write
   * @return true if can write and false if cannot write
   */
  private boolean canWrite(
      Integer variable,
      Integer transactionID,
      Operation operation) {
    for(int i = 1; i <= 10; i++) {
      Site site = sites.get(i);
      if(site.isDown()
          || !site.getLockManager().canWrite(variable, transactionID)) {
        blockOperation(site, variable, operation, transactionID, false);
        return false;
      }
    }
    return true;
  }

  /**
   * Block an operation
   * If this operation is waiting for a variable, then put it into waiting variable list
   * and waits for graph. After that, detect deadlock, abort transaction if there exists
   * If the operation is waiting ofr a down site, then put it into waiting site list
   * @param site used to get lock table and see if it's down
   * @param variable used to find waiting lists of operations
   * @param operation given to block
   * @param transactionID used to build waits for graph
   */
  private void blockOperation(
      Site site,
      Integer variable,
      Operation operation,
      Integer transactionID,
      boolean isReadOperation) {
    if(!site.isDown()) {
      if (isReadOperation) {
        blockRead(site, variable, operation, transactionID);
      } else {
        blockWrite(site, variable, operation, transactionID);
      }
      detectDeadLock();
    } else {
      addToWaitingSiteList(transactionID, site.getIndex());
    }
  }

  /**
   * Print the committed values of all copies of all variables at all 6 sites
   * Sorted per site with all values per site in ascending order by variable name
   */
  private void dump() {
    for (int i = 1; i <= 10; i++) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("site ").append(i).append(" - ");
      Map<Integer, Integer> sortedVariables = sites.get(i)
          .getDataManager()
          .getAllSortedCommittedValues();
      sortedVariables.forEach((key, value) ->
          stringBuilder.append("x").append(key).append(": ").append(value).append(", "));
      System.out.println(stringBuilder);
      IOUtils.writeToOutputFile(stringBuilder.toString());
    }
  }

  /**
   * Get a waiting operation from waiting operation list
   * @param holdVariable given to get a list of operations which are being blocked by this variable
   * @return Operation which will be executed
   */
  private Operation getOneWaitingOperation(Integer holdVariable) {
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
   * Block a transaction to wait for a down site
   * @param transactionID given to block
   * @param site that is down and this transaction is waiting for
   */
  private void addToWaitingSiteList(Integer transactionID, Integer site) {
    if(waitingSites.containsKey(transactionID)) {
      waitingSites.get(transactionID).add(site);
    } else {
      waitingSites.put(transactionID, new ArrayList<>());
      waitingSites.get(transactionID).add(site);
    }
  }

  /**
   * Read and print value of a variable
   * @param site given to read variable
   * @param variable given to read
   * @param readCommittedValue if we should read committed value or current value
   * @param transaction given for print important debugging information
   * @return
   */
  private void readVariable(
      Site site,
      Integer variable,
      boolean readCommittedValue,
      Transaction transaction) {
    Integer value = readCommittedValue ? site.getDataManager().getCommittedValue(variable) :
        site.getDataManager().getCurValue(variable);
    if (transaction.getTransactionStatus() == TransactionStatus.ACTIVE) {
      IOUtils.canReadOutputMessage(transaction, variable, value);
    }
  }

  /**
   * Block a write operation
   * Put this operation to waiting list and wait for graph used for deadlock detection
   * @param site used to get lock table
   * @param variable used to put operation to correct list
   * @param operation given to put into blocking lists
   * @param transactionID given to build wait for graph
   */
  private void blockWrite(
      Site site,
      Integer variable,
      Operation operation,
      Integer transactionID) {
    if (!site.isDown()) {
      putToWaitingOperations(variable, operation);
      checkReadLocks(site, variable, transactionID);
      checkWriteLocks(site, variable, transactionID);
    }
  }

  /**
   * Block a read operation
   * Put this operation to waiting list and wait for graph used for deadlock detection
   * @param site used to get lock table
   * @param variable used to put operation to correct list
   * @param operation given to put into blocking lists
   * @param transactionID given to build wait for graph
   */
  private void blockRead(
      Site site,
      Integer variable,
      Operation operation,
      Integer transactionID) {
    if (!site.isDown()) {
      putToWaitingOperations(variable, operation);
      checkWriteLocks(site, variable, transactionID);
    }
  }

  /**
   * Put an operation to its corresponding waiting list based on variable
   * @param variable this operation is waiting for
   * @param operation which should be blocked
   */
  private void putToWaitingOperations(Integer variable, Operation operation) {
    if (waitingOperations.containsKey(variable)) {
      if (!waitingOperations.get(variable).contains(operation)) {
        waitingOperations.get(variable).add(operation);
      }
    } else {
      waitingOperations.put(variable, new ArrayList<>());
      waitingOperations.get(variable).add(operation);
    }
  }

  /**
   * Build waiting graph
   * @param waitForTransaction which is blocking transactionID
   * @param transactionID which need to wait waitForTransaction
   */
  private void putToWaitingGraph(Integer waitForTransaction, Integer transactionID) {
    if (!waitForTransaction.equals(transactionID)) {
      if (waitsForGraph.containsKey(transactionID)) {
        if (!waitsForGraph.get(transactionID).contains(waitForTransaction)) {
          waitsForGraph.get(transactionID).add(waitForTransaction);
        }
      } else {
        waitsForGraph.put(transactionID, new ArrayList<>());
        waitsForGraph.get(transactionID).add(waitForTransaction);
      }
    }
  }

  /**
   * Check write locks in order to put transaction to wait for graph
   * @param site give to get lock table
   * @param var given to check correct write lock
   * @param transactionID given to put in graph
   */
  private void checkWriteLocks(
      Site site,
      Integer var,
      Integer transactionID) {
    if (site.getLockManager().getWriteLock().containsKey(var)) {
      Integer t = site.getLockManager().getWriteLock().get(var);
      putToWaitingGraph(t, transactionID);
    }
  }

  /**
   * Check read locks in order to put transaction to wait for graph
   * @param site give to get lock table
   * @param var given to check correct write lock
   * @param transactionID given to put in graph
   */
  private void checkReadLocks(
      Site site,
      Integer var,
      Integer transactionID) {
    if (site.getLockManager().getReadLocks().containsKey(var)) {
      site.getLockManager().getReadLocks().get(var).forEach(t -> {
        putToWaitingGraph(t, transactionID);
      });
    }
  }

  /**
   * If there is deadlock, we abort the youngest transaction
   * Set the youngest transaction to 'IS_FINISHED'
   */
  private void detectDeadLock() {
    if(containsDeadLock()) {
      IOUtils.printAndWrite("DeadLock detected, "
          + "we will abort the youngest Transaction in deadlock cycle");
      // Find the youngest transaction
      Integer youngestTransaction = null;
      Integer latestTime = Integer.MIN_VALUE;
      for(Integer visitedTransaction : visitedTransactions) {
        if(getTransaction(visitedTransaction).getTimeStamp() > latestTime) {
          latestTime = getTransaction(visitedTransaction).getTimeStamp();
          youngestTransaction = visitedTransaction;
        }
      }
      abort(youngestTransaction);
      getTransaction(youngestTransaction).setTransactionStatus(TransactionStatus.IS_FINISHED);
    }
  }

  /**
   * Check if there is a deadlock
   * @return true if there exists a deadlock
   */
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

  /**
   * DFS to check if there's a circle from a given node
   * @param transactionID node starts to traverse from
   * @return true if there's a circle, false if there is not
   */
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

  /**
   * Get Transaction based on a operation
   * @param operation given ot get a Transaction object
   * @return a Transaction
   */
  private Transaction getTransaction(Operation operation) {
    List<Transaction> transactions = allTransactions.stream()
        .filter(transaction -> transaction.getName().equals(operation.getTransaction()))
        .collect(Collectors.toList());
    return transactions.get(0);
  }

  /**
   * Get Transaction based on a transaction index
   * @param transactionIndex given ot get a Transaction object
   * @return a Transaction
   */
  private Transaction getTransaction(Integer transactionIndex) {
    List<Transaction> transactions = allTransactions.stream()
        .filter(transaction -> transaction.getName().equals(
            "T" + transactionIndex))
        .collect(Collectors.toList());
    return transactions.get(0);
  }

  /**
   * Commit or abort a transaction based on the parameter given
   * Firstly, we remove this transaction in the waits for graph
   * Then we release all the locks held by this transaction
   * Update committed value to current value if commit
   * Update current value to committed value if abort
   * Finally we wake up operations which are waiting for variables held by this transaction
   * @param transactionID given to commit ot abort
   * @param shouldCommit true if we should commit this transaction,
   * false if we should abort this transaction
   */
  private void commitOrAbort(Integer transactionID, boolean shouldCommit) {

    removeFromWaitsForGraph(transactionID);
    removeFromWaitingOperations(transactionID);

    List<Integer> holdVariables = new ArrayList<>();
    // Iterate all sites and find variables which transaction holds lock on, and remove locks.
    for (int i = 1; i <= 10; ++i) {
      Site site = sites.get(i);

      holdVariables.addAll(findVariables(site, transactionID));

      if (shouldCommit) {
        // Update committed to current values
        updateCommitValues(site, transactionID);
      } else {
        // Revert current value to committed value
        revertCurrentValue(site, transactionID);
      }
      releaseAllLocks(site, transactionID);
    }
    wakeBlockOperations(holdVariables);
  }

  /**
   * Remove this transaction from waitsForGraph
   * @param transactionID given to remove
   */
  private void removeFromWaitsForGraph(Integer transactionID) {
    for(Map.Entry<Integer, List<Integer>> entry : waitsForGraph.entrySet()) {
      entry.getValue().remove(transactionID);
    }
    waitsForGraph.entrySet().removeIf(entry -> entry.getKey().equals(transactionID)
        || entry.getValue().size() == 0);
  }

  /**
   * Remove this transaction from waiting operation lists
   * @param transactionID given to remove
   */
  private void removeFromWaitingOperations(Integer transactionID) {
    for(Map.Entry<Integer, List<Operation>> entry: waitingOperations.entrySet()) {
      entry.getValue().removeIf(operation -> operation.getTransaction().equals("T" + transactionID));
    }
    waitingOperations.entrySet().removeIf(entry -> entry.getValue().size() == 0);
  }

  /**
   * Find variables held by given transaction in a given site
   * @param site given to find hold variables
   * @param transactionID given to find variables
   * @return a list of variables which are being held
   */
  private List<Integer> findVariables(Site site, Integer transactionID) {
    // Add all variables holding by this transaction to a list
    List<Integer> holdVariables = new ArrayList<>();
    site.getLockManager().getReadLocks().forEach((key, value) -> {
      if (value.contains(transactionID) && !holdVariables.contains(key)) {
        holdVariables.add(key);
      }
    });

    site.getLockManager().getWriteLock().forEach((key, value) -> {
      if(value.equals(transactionID) && !holdVariables.contains(key)) {
        holdVariables.add(key);
      }
    });
    return holdVariables;
  }

  /**
   * Update commit value of a variable to its current value
   * @param site used to get lock table
   * @param transactionID used to check if the variable if holding by this transaction
   */
  private void updateCommitValues(Site site, Integer transactionID) {
    site.getLockManager().getWriteLock().forEach((key, value) -> {
      if (value.equals(transactionID)) {
        site.getDataManager().updateToCurValue(key);
      }
    });
  }

  /**
   * Revert current value of a variable to its commit value
   * @param site used to get lock table
   * @param transactionID used to check if the variable if holding by this transaction
   */
  private void revertCurrentValue(Site site, Integer transactionID) {
    site.getLockManager().getWriteLock().forEach((key, value) -> {
      if (value.equals(transactionID)) {
        site.getDataManager().updateToCommittedValue(key);
      }
    });
  }

  /**
   * Release all the locks
   * @param site given to get lock table
   * @param transactionID used to release locks
   */
  private void releaseAllLocks(Site site, Integer transactionID) {
    site.getLockManager().getReadLocks().forEach((key, value) -> value.remove(transactionID));
    site.getLockManager()
        .getWriteLock().entrySet().removeIf(entry -> entry.getValue().equals(transactionID));
    site.getLockManager().getReadLocks().entrySet().removeIf(entry -> entry.getValue().size() == 0);
  }

  /**
   * Wake waiting operations which are waiting for
   * the released variables held by the transaction
   * @param holdVariables which are held by the transaction before
   */
  private void wakeBlockOperations(List<Integer> holdVariables) {
    holdVariables.forEach(holdVariable -> {
      if (waitingOperations.containsKey(holdVariable)) {
        Operation waitingOperation = waitingOperations.get(holdVariable).get(0);
        boolean flag = true;
        while (waitingOperation != null && flag) {
          if (waitingOperation.getName().equals(IOUtils.READ)) {
            if (read(waitingOperation)) {
              waitingOperation = getOneWaitingOperation(holdVariable);
            } else {
              flag = false;
            }
          } else if (waitingOperation.getName().equals(IOUtils.WRITE)) {
            if (write(waitingOperation)) {
              IOUtils.canWriteOutputMessage(waitingOperation);
              waitingOperation = getOneWaitingOperation(holdVariable);
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
}
