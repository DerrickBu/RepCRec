package cs.nyu.edu.adb;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class DataManager {

  //variable name -> (last committed value, current value)
  private Map<Integer, ImmutablePair<Integer, Integer>> variables;

  public DataManager() {
    variables = new HashMap<>();
  }

  /**
   * Insert a new value for a variable
   * @param variable given to add value to
   * @param currentValue used to give to the variable
   */
  public void insertValue(Integer variable, Integer currentValue) {
    variables.put(variable, new ImmutablePair<>(currentValue, currentValue));
  }

  /**
   * Update current value of an existing variable
   * @param variable given to update value
   * @param updateValue for the given variable
   */
  public void updateValue(Integer variable, Integer updateValue) {
    Integer committedValue = variables.get(variable).getKey();
    variables.put(variable, new ImmutablePair<>(committedValue, updateValue));
  }

  /**
   * Revert the current value to its committed value for given variable
   * @param variable given to revert its current value
   */
  public void updateToCommittedValue(Integer variable) {
    Integer committedValue = variables.get(variable).getKey();
    variables.put(variable, new ImmutablePair<>(committedValue, committedValue));
  }

  /**
   * Update the committed value to its current value for given variable
   * @param variable given to update
   */
  public void updateToCurValue(Integer variable) {
    Integer curValue = variables.get(variable).getValue();
    variables.put(variable, new ImmutablePair<>(curValue, curValue));
  }

  /**
   * Get current value for the given variable
   * @param variable given to get current value
   * @return current value of a variable
   */
  public Integer getCurValue(Integer variable) {
    return variables.get(variable).getValue();
  }

  /**
   * Get all committed values which are sorted by their variable index
   * @return values of all variables sorted by variables' indexes
   */
  public Map<Integer, Integer> getAllSortedCommittedValues() {

    LinkedHashMap<Integer, Integer> sortedMap = new LinkedHashMap<>();
    variables.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByKey())
        .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue().getKey()));

    return sortedMap;
  }

  /**
   * Get the committed value for a given variable
   * @param variable given to get committed value
   * @return committed value of a variable
   */
  public Integer getCommittedValue(Integer variable) {
    return variables.get(variable).getKey();
  }

}
