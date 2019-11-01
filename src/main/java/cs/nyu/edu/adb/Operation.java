package cs.nyu.edu.adb;

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

  public void setName(String name) {
    this.name = name;
  }

  public void setTransaction(String transaction) {
    this.transaction = transaction;
  }

  public void setVariable(Integer variable) {
    this.variable = variable;
  }

  public void setWritesToValue(Integer writesToValue) {
    this.writesToValue = writesToValue;
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

  public void setSite(Integer site) {
    this.site = site;
  }
}
