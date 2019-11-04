package cs.nyu.edu.adb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LockManager {

  // variable -> transaction index
  Map<Integer, List<Integer>> readLocks;
  Map<Integer, Integer> writeLock;

  public boolean canRead(Integer variable, Integer transaction) {
    if(!writeLock.containsKey(variable) || writeLock.get(variable) == transaction) {
      if(!readLocks.containsKey(variable)) {
        readLocks.put(variable, Arrays.asList(transaction));
      } else if(!readLocks.get(variable).contains(transaction)) {
        readLocks.get(variable).add(transaction);
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean canWrite(Integer variable, Integer transaction) {
    if(readLocks.containsKey(variable)
        && readLocks.get(variable).size() == 1
        && readLocks.get(variable).contains(transaction) ||
        (!readLocks.containsKey(variable) && !writeLock.containsKey(variable))) {
      writeLock.put(variable, transaction);
      return true;
    } else {
      return false;
    }
  }

}
