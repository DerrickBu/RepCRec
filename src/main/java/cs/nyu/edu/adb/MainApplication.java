package cs.nyu.edu.adb;

public class MainApplication {

  public static void main(String[] args) {

    if(args.length == 0) {
      throw new IllegalArgumentException("Should provide a filename");
    }

    // Parse input file, get a list of operations
    IOUtils ioUtils = new IOUtils(args[0]);
    ioUtils.parseFile();

    // send all parsed operations to transaction manager, and start running
    TransactionManager transactionManager = new TransactionManager(ioUtils.operations);
    transactionManager.run();

  }
}
