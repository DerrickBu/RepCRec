package cs.nyu.edu.adb;

import java.util.HashMap;
import java.util.Map;
import javafx.util.Pair;

public class DataManager {

  //variable name -> (last commited value, curvalue)
  private Map<Integer, Pair<Integer, Integer>> variables;

  public DataManager() {
    variables = new HashMap<>();
  }

  public void insertValue(Integer variable, Integer curvalue) {
    variables.put(variable, new Pair<>(curvalue, curvalue));
  }

  public void updateValue(Integer variable, Integer updateValue) {
    Integer committedValue = variables.get(variable).getKey();
    variables.put(variable, new Pair<>(committedValue, updateValue));
  }

  public Integer getCurValue(Integer variable) {
    return variables.get(variable).getValue();
  }

  public Integer getCommittedValue(Integer variable) {
    return variables.get(variable).getKey();
  }

}
