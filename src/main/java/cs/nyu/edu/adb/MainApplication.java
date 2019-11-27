package cs.nyu.edu.adb;

import java.io.IOException;

public class MainApplication {

  public static void main(String[] args) {

    if(args.length == 0) {
      for (int i = 1; i < 12; i++) {
        // Parse input file, get a list of operations
        IOUtils ioUtils = new IOUtils();
        IOUtils.inputFile = String.format("./TestInput/test%s.txt", i);
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
    } else {

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
}
