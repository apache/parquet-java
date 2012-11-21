package redelm.pig.converter;

import redelm.pig.TupleConversionException;
import redelm.schema.GroupType;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class BagConverter extends Converter {

  private DataBag currentBag;
  private TupleConverter child;

  BagConverter(GroupType redelmSchema, FieldSchema pigSchema, Converter parent) throws FrontendException {
    super(parent);
    child = new TupleConverter(redelmSchema.getType(0).asGroupType(), pigSchema.schema.getField(0).schema, this);
  }

  @Override
  public void start() {
    currentBag = new NonSpillableDataBag();
  }

  @Override
  public void startField(String field, int index) {
    assert index == 0;
  }

  @Override
  public void endField(String field, int index) {
    assert index == 0;
  }

  @Override
  public Converter startGroup() {
    return child;
  }

  @Override
  public void endGroup() {
    currentBag.add(child.get());
  }

  @Override
  public DataBag get() {
    return currentBag;
  }

  @Override
  public void set(Object value) {
    throw new TupleConversionException("bag can not contain primitive value " + value);
  }

}
