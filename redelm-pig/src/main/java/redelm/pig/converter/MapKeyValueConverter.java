package redelm.pig.converter;

import redelm.schema.GroupType;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class MapKeyValueConverter extends Converter {
  private TupleConverter value;
  private int currentField;
  private String currentKey;

  MapKeyValueConverter(GroupType redelmSchema, Schema pigSchema, MapConverter parent) throws FrontendException {
    super(parent);
    assert redelmSchema.getFieldCount() == 2;
    assert redelmSchema.getType(0).getName().equals("key");
    assert redelmSchema.getType(1).getName().equals("value");
    value = new TupleConverter(redelmSchema.getType(1).asGroupType(), pigSchema, this);
  }

  @Override
  public void start() {
    this.currentKey = null;
  }

  @Override
  public void startField(String field, int index) {
    currentField = index;
  }

  @Override
  public void endField(String field, int index) {
    assert currentField == index;
    currentField = -1;
  }

  @Override
  public Converter startGroup() {
    assert currentField == 1;
    return value;
  }

  @Override
  public void endGroup() {
  }

  @Override
  public Tuple get() {
    return value.get();
  }

  @Override
  public void set(Object value) {
    assert currentField == 0;
    currentKey = (String)value;
  }

  public String getKey() {
    return currentKey;
  }

}
