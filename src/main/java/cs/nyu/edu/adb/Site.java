package cs.nyu.edu.adb;

import java.util.concurrent.locks.Lock;

public class Site {

  public LockManager lockManager;
  public DataManager dataManager;
  Boolean isDown;

  public Site() {
    isDown = false;
    dataManager = new DataManager();
    lockManager = new LockManager();
  }

  public LockManager getLockManager() {
    return lockManager;
  }

  public DataManager getDataManager() {
    return dataManager;
  }

  public void setLockManager(LockManager lockManager) {
    this.lockManager = lockManager;
  }

  public void setDataManager(DataManager dataManager) {
    this.dataManager = dataManager;
  }
}
