package redelm.pig.converter;

import java.util.HashMap;
import java.util.Map;

import redelm.schema.GroupType;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class MapConverter extends Converter {

  private Map<String, Tuple> currentMap;
  private MapKeyValueConverter keyValue;

  MapConverter(GroupType redelmSchema, FieldSchema pigSchema, Converter parent) throws FrontendException {
    super(parent);
    keyValue = new MapKeyValueConverter(redelmSchema.getType(0).asGroupType(), pigSchema.schema.getField(0).schema, this);
  }

  @Override
  public void start() {
    currentMap = new HashMap<String, Tuple>();
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
    return keyValue;
  }

  @Override
  public void endGroup() {
    currentMap.put(keyValue.getKey(), keyValue.get());
  }

  @Override
  public Map<String, Tuple> get() {
    return currentMap;
  }

  @Override
  public void set(Object value) {
    throw new UnsupportedOperationException("maps contain only key/value groups");
  }

}
