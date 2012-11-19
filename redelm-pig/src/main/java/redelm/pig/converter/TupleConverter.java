package redelm.pig.converter;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class TupleConverter extends Converter {

  private static final TupleFactory TF = TupleFactory.getInstance();

  private Tuple currentTuple;
  private int schemaSize;
  private int currentField;
  private Converter[] converters;

  @Override
  public Converter startGroup() {
    return converters[currentField];
  }

  @Override
  public void endGroup() {
    try {
      currentTuple.set(currentField, converters[currentField].get());
    } catch (ExecException e) {
      throw new RuntimeException(e);
    }
  }

  private Tuple getGroup() {
    return currentTuple;
  }

  @Override
  public void startField(String field, int index) {
    this.currentField = index;
  }

  @Override
  public void endField(String field, int index) {
    this.currentField = -1;
  }

  @Override
  public void start() {
    currentTuple = TF.newTuple(schemaSize);

  }

  @Override
  public Converter end() {
    return getParent();
  }

  @Override
  public Object get() {
    return currentTuple;
  }

  @Override
  public void set(Object value) {
    try {
      currentTuple.set(currentField, value);
    } catch (ExecException e) {
      throw new RuntimeException(e);
    }
  }

}
