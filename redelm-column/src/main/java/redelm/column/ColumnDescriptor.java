package redelm.column;

import java.util.Arrays;

import redelm.schema.PrimitiveType.Primitive;

public class ColumnDescriptor implements Comparable<ColumnDescriptor> {

  private final String[] path;
  private final Primitive type;

  public ColumnDescriptor(String[] path, Primitive type) {
    super();
    this.path = path;
    this.type = type;
  }

  public String[] getPath() {
    return path;
  }

  public Primitive getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(path);
  }

  @Override
  public boolean equals(Object obj) {
    return Arrays.equals(path, ((ColumnDescriptor)obj).path);
  }

  @Override
  public int compareTo(ColumnDescriptor o) {
    for (int i = 0; i < path.length; i++) {
      int compareTo = path[i].compareTo(o.path[i]);
      if (compareTo != 0) {
        return compareTo;
      }
    }
    return 0;
  }

  @Override
  public String toString() {
    return Arrays.toString(path)+" "+type;
  }
}
