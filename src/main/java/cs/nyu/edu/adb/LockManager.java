package cs.nyu.edu.adb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LockManager {

  // variable -> transaction index
  private Map<Integer, List<Integer>> readLocks;
  private Map<Integer, Integer> writeLock;

  public LockManager() {
    readLocks = new HashMap<>();
    writeLock = new HashMap<>();
  }

  public Map<Integer, List<Integer>> getReadLocks() {
    return readLocks;
  }

  public Map<Integer, Integer> getWriteLock() {
    return writeLock;
  }

  /**
   * Check if we could read variable from this site
   * @param variable given to read
   * @param transaction given to check lock conflicts
   * @return true if can read, false if we can't
   */
  public boolean canRead(Integer variable, Integer transaction) {
    return (!writeLock.containsKey(variable) || writeLock.get(variable).equals(transaction));
  }

  /**
   * Add read lock to the given variable if we can read
   * @param variable given to add lock on
   * @param transaction given to add lock
   */
  public void addReadLock(Integer variable, Integer transaction) {
    if(!readLocks.containsKey(variable)) {
      readLocks.put(variable, new ArrayList<>());
      readLocks.get(variable).add(transaction);
    } else if(!readLocks.get(variable).contains(transaction)) {
      readLocks.get(variable).add(transaction);
    }
  }

  /**
   * Add write lock to the given variable if we can write
   * @param variable given to add lock on
   * @param transaction given to add lock
   */
  public void addWriteLock(Integer variable, Integer transaction) {
    writeLock.put(variable, transaction);
  }

  /**
   * Check if we could write variable to this site
   * @param variable given to write
   * @param transaction given to check lock conflicts
   * @return true if can write, false if we can't
   */
  public boolean canWrite(Integer variable, Integer transaction) {
    return (readLocks.containsKey(variable)
        && readLocks.get(variable).size() == 1
        && readLocks.get(variable).contains(transaction) ||
        (!readLocks.containsKey(variable) && !writeLock.containsKey(variable)));
  }

}
