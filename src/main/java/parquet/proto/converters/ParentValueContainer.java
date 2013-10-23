package parquet.proto.converters;

public abstract class ParentValueContainer {

  /**
   * Adds the value to the parent.
   */
  public abstract void add(Object value);

}