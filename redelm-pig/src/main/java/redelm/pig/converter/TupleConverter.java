package redelm.pig.converter;

import redelm.pig.TupleConversionException;
import redelm.schema.GroupType;
import redelm.schema.Type;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TupleConverter extends Converter {

  private static final TupleFactory TF = TupleFactory.getInstance();

  private Tuple currentTuple;
  private int schemaSize;
  private int currentField;
  private Converter[] converters;

  TupleConverter(GroupType redelmSchema, Schema pigSchema, Converter parent) throws FrontendException {
    super(parent);
    this.schemaSize = redelmSchema.getFieldCount();
    this.converters = new Converter[this.schemaSize];
    for (int i = 0; i < converters.length; i++) {
      FieldSchema field = pigSchema.getField(i);
      Type type = redelmSchema.getType(i);
      switch (field.type) {
      case DataType.BAG:
        converters[i] = new BagConverter(type.asGroupType(), field, this);
        break;
      case DataType.MAP:
        converters[i] = new MapConverter(type.asGroupType(), field, this);
        break;
      case DataType.TUPLE:
        converters[i] = new TupleConverter(type.asGroupType(), field.schema, this);
        break;
      default:
        converters[i] = null;
      }
    }
  }

  @Override
  public Converter startGroup() {
    return converters[currentField];
  }

  @Override
  public void endGroup() {
    try {
      currentTuple.set(currentField, converters[currentField].get());
    } catch (ExecException e) {
      throw new TupleConversionException(
          "Could not set the child value to the currentTuple " + currentTuple +
          " at " + currentField, e);
    }
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
  public Tuple get() {
    return currentTuple;
  }

  @Override
  public void set(Object value) {
    try {
      currentTuple.set(currentField, value);
    } catch (ExecException e) {
      throw new TupleConversionException(
          "Could not set " + value +
          " to current tuple " + currentTuple + " at " + currentField, e);
    }
  }

}
