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
  public static String inputFile;
  public static String outputFile;
  public List<Operation> operations;

  public IOUtils() {

  }

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

  public static void createOutputFile() throws IOException{
    if(inputFile == null) {
      throw new IOException("Should provide a input file");
    }
    String[] filePath = inputFile.split("/");
    StringBuilder outfile = new StringBuilder()
        .append(System.getProperty("user.dir"))
        .append("/TestOutput/")
        .append(filePath[filePath.length - 1]);
    IOUtils.outputFile = outfile.toString();
    File file = new File(outputFile);
    if(Files.exists(Paths.get(outfile.toString()))) {
      file.delete();
    }
  }

  public static void writeToOutputFile(String str) {
    try(FileWriter fw = new FileWriter(outputFile, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw)) {
      out.println(str);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
