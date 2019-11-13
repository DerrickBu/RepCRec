package cs.nyu.edu.adb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LockManager {

  // variable -> transaction index
  public Map<Integer, List<Integer>> readLocks;
  public Map<Integer, Integer> writeLock;

  public LockManager() {
    readLocks = new HashMap<>();
    writeLock = new HashMap<>();
  }

  public boolean canRead(Integer variable, Integer transaction) {
    if(!writeLock.containsKey(variable) || writeLock.get(variable) == transaction) {
      if(!readLocks.containsKey(variable)) {
        readLocks.put(variable, new ArrayList<>());
        readLocks.get(variable).add(transaction);
      } else if(!readLocks.get(variable).contains(transaction)) {
        readLocks.get(variable).add(transaction);
      }
      return true;
    } else {
      return false;
    }
  }

  public void write(Integer variable, Integer transaction) {
    writeLock.put(variable, transaction);
  }

  public boolean canWrite(Integer variable, Integer transaction) {
    if(readLocks.containsKey(variable)
        && readLocks.get(variable).size() == 1
        && readLocks.get(variable).contains(transaction) ||
        (!readLocks.containsKey(variable) && !writeLock.containsKey(variable))) {
      return true;
    } else {
      return false;
    }
  }

}
