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
        String seperateStrings[] = line.trim().split("\\(|\\)|,");
        if(seperateStrings[0].equals(BEGIN)
            || seperateStrings[0].equals(END)
            || seperateStrings[0].equals(BEGIN_RO)
            || seperateStrings[0].equals(DUMP)) {
          return new Operation.Builder(seperateStrings[0])
              .transaction(seperateStrings[1])
              .build();
        } else if(seperateStrings[0].equals(READ)) {
          return new Operation.Builder(seperateStrings[0])
              .transaction(seperateStrings[1])
              .variable(Integer.valueOf(seperateStrings[2].substring(1)))
              .build();
        } else if(seperateStrings[0].equals(WRITE)) {
          return new Operation.Builder(seperateStrings[0])
              .transaction(seperateStrings[1])
              .variable(Integer.valueOf(seperateStrings[2].substring(1)))
              .writesToValue(Integer.valueOf(seperateStrings[3]))
              .build();
        } else if(seperateStrings[0].equals(FAIL)
            || seperateStrings[0].equals(RECOVER)) {
          return new Operation.Builder(seperateStrings[0])
              .site(Integer.valueOf(seperateStrings[1]))
              .build();
        } else {
          throw new UnsupportedOperationException("This opetation is not being supported");
        }
      }).collect(Collectors.toList());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // TODO: output utils
}
