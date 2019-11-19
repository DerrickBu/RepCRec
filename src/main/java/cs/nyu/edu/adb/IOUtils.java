package cs.nyu.edu.adb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IOUtils {

  public static final String BEGIN = "begin";
  public static final String END = "end";
  public static final String BEGIN_RO = "beginRO";
  public static final String READ = "R";
  public static final String WRITE = "W";
  public static final String DUMP = "dump";
  public static final String FAIL = "fail";
  public static final String RECOVER = "recover";
  public static final String OUTPUT_DIRECTORY = "/TestOutput/";
  public static final String REGEX = "/";
  public static final String USER_DIR = "user.dir";
  public static String inputFile;
  public static String outputFile;
  public List<Operation> operations;

  public IOUtils() {

  }

  /**
   * Parse a input file line by line to a list of operations
   * Could have blank after each segment
   */
  public void parseFile() {
    try(Stream<String> stream = Files.lines(Paths.get(inputFile))) {
      this.operations = stream
          .filter(line -> !line.startsWith("//"))
          .map(line -> {
        String[] separateStrings = line.trim().split("\\(|\\)|,");
        if(separateStrings[0].equals(BEGIN)
            || separateStrings[0].equals(END)
            || separateStrings[0].equals(BEGIN_RO)) {
          return new Operation.Builder(separateStrings[0].trim())
              .transaction(separateStrings[1].trim())
              .build();
        } else if(separateStrings[0].equals(DUMP)){
          return new Operation.Builder(separateStrings[0].trim())
              .build();
        } else if(separateStrings[0].equals(READ)) {
          return new Operation.Builder(separateStrings[0].trim())
              .transaction(separateStrings[1].trim())
              .variable(Integer.valueOf(separateStrings[2].trim().substring(1)))
              .build();
        } else if(separateStrings[0].equals(WRITE)) {
          return new Operation.Builder(separateStrings[0].trim())
              .transaction(separateStrings[1].trim())
              .variable(Integer.valueOf(separateStrings[2].trim().substring(1)))
              .writesToValue(Integer.valueOf(separateStrings[3].trim()))
              .build();
        } else if(separateStrings[0].equals(FAIL)
            || separateStrings[0].equals(RECOVER)) {
          return new Operation.Builder(separateStrings[0].trim())
              .site(Integer.valueOf(separateStrings[1].trim()))
              .build();
        } else {
          throw new UnsupportedOperationException("This operation is not being supported");
        }
      }).collect(Collectors.toList());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Create an output file under 'TestOutput' directory
   * The name of the output file would be the same as the name of input file
   * @throws IOException if the input file is null
   */
  public static void createOutputFile() throws IOException{
    if(inputFile == null) {
      throw new IOException("Should provide a input file");
    }
    String[] filePath = inputFile.split(REGEX);
    StringBuilder outfile = new StringBuilder()
        .append(System.getProperty(USER_DIR))
        .append(OUTPUT_DIRECTORY)
        .append(filePath[filePath.length - 1]);
    IOUtils.outputFile = outfile.toString();
    File file = new File(outputFile);
    if(Files.exists(Paths.get(outfile.toString()))) {
      file.delete();
    }
  }

  /**
   * Write a string to the output file, one string a line
   * @param str which will be written to the output file
   */
  public static void writeToOutputFile(String str) {
    try(FileWriter fw = new FileWriter(outputFile, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw)) {
      out.println(str);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Print and write output message when we begin a transaction
   * @param operation used to get transaction name
   */
  public static void beginOutputMessage(Operation operation) {
    String outputMessage = String.format("%s begins",
        operation.getTransaction());
    printAndWrite(outputMessage);
  }

  /**
   * Print and write output message when we begin a read-only transaction
   * @param operation used to get transaction name
   */
  public static void beginROOutputMessage(Operation operation) {
    String outputMessage = String.format("Read-only transaction %s begins",
        operation.getTransaction());
    printAndWrite(outputMessage);
  }

  /**
   * Print and write output message when a site fails
   * @param operation used to get site index
   */
  public static void failOutputMessage(Operation operation) {
    String outputMessage = String.format("Site %s fails",
        operation.getSite());
    printAndWrite(outputMessage);
  }

  /**
   * Print and write output message when a site recovers
   * @param operation used to get site index
   */
  public static void recoverOutputMessage(Operation operation) {
    String outputMessage = String.format("Site %s recovers",
        operation.getSite());
    printAndWrite(outputMessage);
  }

  /**
   * Print and write output message when we abort a transaction
   * @param transactionID info to print and write
   */
  public static void abortOutputMessage(Integer transactionID) {
    String outputMessage = String.format("%s aborts",
        "T" + transactionID);
    printAndWrite(outputMessage);
  }

  /**
   * Print and write output message when we commit a transaction
   * @param transactionID info to print and write
   */
  public static void commitOutputMessage(Integer transactionID) {
    String outputMessage = String.format("%s commits",
        "T" + transactionID);
    printAndWrite(outputMessage);
  }

  /**
   * Print and write message to output file if we could write variable to a new value
   * @param operation used to get important message like variable name and etc
   */
  public static void canWriteOutputMessage(Operation operation) {
    String outputMessage = buildWriteOutputString(operation,
        "%s can write variable %s to new value %s");
    printAndWrite(outputMessage);
  }

  /**
   * Print and write message to output file if we could not write variable to a new value
   * @param operation used to get important message like variable name and etc
   */
  public static void cannotWriteOutputMessage(Operation operation) {
    String outputMessage = buildWriteOutputString(operation,
        "%s cannot write variable %s to new value %s");
    printAndWrite(outputMessage);
  }

  /**
   * Print and write message to output file if we could read variable to a new value
   * @param transaction used to get transaction name
   * @param variable used to get variable name
   * @param value used to get value of the variable
   */
  public static void canReadOutputMessage(
      Transaction transaction,
      Integer variable,
      Integer value) {
    String outputMessage = String.format("%s can read variable %s, the value is %s",
        transaction.getName(), "x" + variable, value);
    printAndWrite(outputMessage);
  }

  /**
   * Print and write message to output file if we could not read variable to a new value
   * @param operation used to get important message like variable name and etc
   */
  public static void cannotReadOutputMessage(Operation operation) {
    String outputMessage = String.format("%s cannot read variable %s",
        operation.getTransaction(), operation.getVariable().toString());
    printAndWrite(outputMessage);
  }

  /**
   * Build output message for write operation
   * @param operation used to get important messages like transaction name and etc.
   * @param str output message format
   * @return Well built output message
   */
  private static String buildWriteOutputString(Operation operation, String str) {
    return String.format(str,
        operation.getTransaction(),
        operation.getVariable().toString(),
        operation.getWritesToValue().toString());
  }

  /**
   * Print the output message to console and write it to output file
   * @param outputMessage given to print and write
   */
  public static void printAndWrite(String outputMessage) {
    System.out.println(outputMessage);
    writeToOutputFile(outputMessage);
  }

}
