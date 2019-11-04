package cs.nyu.edu.adb;

import java.util.concurrent.locks.Lock;

public class Site {

  private LockManager lockManager;
  private DataManager dataManager;
  public boolean isDown;

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
}
