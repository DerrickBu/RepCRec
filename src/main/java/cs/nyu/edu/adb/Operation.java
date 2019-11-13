package cs.nyu.edu.adb;

import java.util.Objects;

public class Operation {

  private String name;
  private String transaction;
  private Integer variable;
  private Integer writesToValue;
  private Integer site;

  public static class Builder {

    private String name;
    private String transaction;
    private Integer variable;
    private Integer writesToValue;
    private Integer site;

    Builder(String name) {
      this.name = name;
    }

    public Builder transaction(String transaction) {
      this.transaction = transaction;
      return this;
    }

    public Builder variable(Integer variable) {
      this.variable = variable;
      return this;
    }

    public Builder writesToValue(Integer writesToValue) {
      this.writesToValue = writesToValue;
      return this;
    }

    public Builder site(Integer site) {
      this.site = site;
      return this;
    }

    public Operation build() {
      return new Operation(this);
    }

  }

  private Operation(Builder builder) {
    this.name = builder.name;
    this.transaction = builder.transaction;
    this.variable = builder.variable;
    this.writesToValue = builder.writesToValue;
    this.site = builder.site;
  }

  public String getTransaction() {
    return transaction;
  }

  public Integer getVariable() {
    return variable;
  }

  public Integer getWritesToValue() {
    return writesToValue;
  }

  public String getName() {
    return name;
  }

  public Integer getSite() {
    return site;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Operation)) {
      return false;
    }
    Operation c = (Operation) o;
    return Objects.equals(c.name, name)
        && Objects.equals(c.transaction, transaction)
        && Objects.equals(c.variable, variable)
        && Objects.equals(c.writesToValue, writesToValue)
        && Objects.equals(c.site, site);
  }

}
