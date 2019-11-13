package cs.nyu.edu.adb;

import java.io.IOException;
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
  private String fileName;
  List<Operation> operations;

  public IOUtils(String fileName) {
    this.fileName = fileName;
  }

  public void parseFile() {
    try(Stream<String> stream = Files.lines(Paths.get(fileName))) {
      this.operations = stream.map(line -> {
        String[] separateStrings = line.trim().split("\\(|\\)|,");
        if(separateStrings[0].equals(BEGIN)
            || separateStrings[0].equals(END)
            || separateStrings[0].equals(BEGIN_RO)) {
          return new Operation.Builder(separateStrings[0])
              .transaction(separateStrings[1])
              .build();
        } else if(separateStrings[0].equals(DUMP)){
          return new Operation.Builder(separateStrings[0])
              .build();
        } else if(separateStrings[0].equals(READ)) {
          return new Operation.Builder(separateStrings[0])
              .transaction(separateStrings[1])
              .variable(Integer.valueOf(separateStrings[2].substring(1)))
              .build();
        } else if(separateStrings[0].equals(WRITE)) {
          return new Operation.Builder(separateStrings[0])
              .transaction(separateStrings[1])
              .variable(Integer.valueOf(separateStrings[2].substring(1)))
              .writesToValue(Integer.valueOf(separateStrings[3]))
              .build();
        } else if(separateStrings[0].equals(FAIL)
            || separateStrings[0].equals(RECOVER)) {
          return new Operation.Builder(separateStrings[0])
              .site(Integer.valueOf(separateStrings[1]))
              .build();
        } else {
          throw new UnsupportedOperationException("This operation is not being supported");
        }
      }).collect(Collectors.toList());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
