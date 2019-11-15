package cs.nyu.edu.adb;

import java.io.IOException;

public class MainApplication {

  public static void main(String[] args) {

    if(args.length == 0) {
      throw new IllegalArgumentException("Should provide a filename");
    }

    // Parse input file, get a list of operations
    IOUtils ioUtils = new IOUtils();
    IOUtils.inputFile = args[0];
    try {
      IOUtils.createOutputFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    ioUtils.parseFile();

    // send all parsed operations to transaction manager, and start running
    TransactionManager transactionManager = new TransactionManager(ioUtils.operations);
    transactionManager.run();
  }
}
