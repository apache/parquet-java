package parquet.data.materializer;

import java.util.ArrayList;
import java.util.List;

import parquet.data.Group;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

class GroupConverterField {

  static public Class<? extends GroupConverterField> getGroupConverterField(Type t) {
    boolean repeated = t.isRepetition(Repetition.REPEATED);
    if (t.isPrimitive()) {
      PrimitiveType primitiveType = t.asPrimitiveType();
      switch (primitiveType.getPrimitiveTypeName()) {
      case INT32:
        return repeated ? RepeatedIntField.class : IntField.class;
      case INT64:
        return repeated ? RepeatedLongField.class : LongField.class;
      case FLOAT:
        return repeated ? RepeatedFloatField.class : FloatField.class;
      case DOUBLE:
        return repeated ? RepeatedDoubleField.class : DoubleField.class;
      case BINARY:
        return repeated ? RepeatedBinaryField.class : BinaryField.class;
      default:
        throw new RuntimeException(t.toString());
      }
    } else {
      return repeated ? GroupListField.class : GroupField.class;
    }
  }

  public static class GroupListField<T extends Group> extends GroupConverterField {
    private final List<T> list = new ArrayList<T>();

    public void add(T value) {
      list.add(value);
    }

    public Group[] get(Group[] in) {
      Group[] array = list.toArray(in);
      list.clear();
      return array;
    }

    public int size() {
      return list.size();
    }
  }
  public static class GroupField<T extends Group> extends GroupConverterField {
    private T value;

    public void add(T value) {
      this.value = value;
    }

    public boolean isSet() {
      return value != null;
    }

    public T get() {
      T result = value;
      value = null;
      return result;
    }
  }


  public static class IntField extends GroupConverterField {
    boolean isSet = false;
    int value;

    public void add(int value) {
      this.isSet = true;
      this.value = value;
    }

    public boolean isSet() {
      return isSet;
    }

    public int get() {
      isSet = false;
      return value;
    }
  }

  public static class LongField extends GroupConverterField {
    boolean isSet = false;
    long value;

    public void add(long value) {
      this.isSet = true;
      this.value = value;
    }

    public boolean isSet() {
      return isSet;
    }

    public long get() {
      isSet = false;
      return value;
    }
  }

  public static class FloatField extends GroupConverterField {
    boolean isSet = false;
    float value;

    public void add(float value) {
      this.isSet = true;
      this.value = value;
    }

    public boolean isSet() {
      return isSet;
    }

    public float get() {
      isSet = false;
      return value;
    }
  }

  public static class DoubleField extends GroupConverterField {
    boolean isSet = false;
    double value;

    public void add(double value) {
      this.isSet = true;
      this.value = value;
    }

    public boolean isSet() {
      return isSet;
    }

    public double get() {
      isSet = false;
      return value;
    }
  }

  public static class BinaryField extends GroupConverterField {
    Binary value;

    public void add(Binary value) {
      this.value = value;
    }

    public boolean isSet() {
      return value != null;
    }

    public Binary get() {
      Binary r = value;
      value = null;
      return r;
    }
  }

  public static class RepeatedIntField extends GroupConverterField {
    private final List<Integer> values = new ArrayList<Integer>();

    public void add(int value) {
      this.values.add(value);
    }

    public int[] get(int[] array) {
      for (int i = 0; i < array.length; i++) {
        array[i] = values.get(i);
      }
      values.clear();
      return array;
    }

    public int size() {
      return values.size();
    }
  }

  public static class RepeatedLongField extends GroupConverterField {
    private final List<Long> values = new ArrayList<Long>();

    public void add(long value) {
      this.values.add(value);
    }

    public long[] get(long[] array) {
      for (int i = 0; i < array.length; i++) {
        array[i] = values.get(i);
      }
      values.clear();
      return array;
    }

    public int size() {
      return values.size();
    }
  }

  public static class RepeatedFloatField extends GroupConverterField {
    private final List<Float> values = new ArrayList<Float>();

    public void add(float value) {
      this.values.add(value);
    }

    public float[] get(float[] array) {
      for (int i = 0; i < array.length; i++) {
        array[i] = values.get(i);
      }
      values.clear();
      return array;
    }

    public int size() {
      return values.size();
    }
  }

  public static class RepeatedDoubleField extends GroupConverterField {
    private final List<Double> values = new ArrayList<Double>();

    public void add(double value) {
      this.values.add(value);
    }

    public double[] get(double[] array) {
      for (int i = 0; i < array.length; i++) {
        array[i] = values.get(i);
      }
      values.clear();
      return array;
    }

    public int size() {
      return values.size();
    }
  }

  public static class RepeatedBinaryField extends GroupConverterField {
    private final List<Binary> values = new ArrayList<Binary>();

    public void add(Binary value) {
      this.values.add(value);
    }

    public Binary[] get(Binary[] in) {
      in = values.toArray(in);
      values.clear();
      return in;
    }

    public int size() {
      return values.size();
    }
  }
}
