package cs.nyu.edu.adb;

public class Site {

  private LockManager lockManager;
  private DataManager dataManager;
  private boolean isDown;
  private Integer index;

  public Site(Integer index) {
    isDown = false;
    dataManager = new DataManager();
    lockManager = new LockManager();
    this.index = index;
  }

  public Integer getIndex() {
    return index;
  }

  public boolean isDown() {
    return isDown;
  }

  public void setIsDown(boolean isDown) {
    this.isDown = isDown;
  }

  public LockManager getLockManager() {
    return lockManager;
  }

  public DataManager getDataManager() {
    return dataManager;
  }
}
