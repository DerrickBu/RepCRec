package cs.nyu.edu.adb;

import java.util.HashMap;
import java.util.LinkedHashMap;
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

  public void updateToCommittedValue(Integer variable) {
    Integer committedValue = variables.get(variable).getKey();
    variables.put(variable, new Pair<>(committedValue, committedValue));
  }

  public void updateToCurValue(Integer variable) {
    Integer curValue = variables.get(variable).getValue();
    variables.put(variable, new Pair<>(curValue, curValue));
  }

  public Integer getCurValue(Integer variable) {
    return variables.get(variable).getValue();
  }

  public Map<Integer, Integer> getAllSortedCommittedValues() {

    LinkedHashMap<Integer, Integer> sortedMap = new LinkedHashMap<>();
    variables.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByKey())
        .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue().getKey()));

    return sortedMap;
  }

  public Integer getCommittedValue(Integer variable) {
    return variables.get(variable).getKey();
  }

}
