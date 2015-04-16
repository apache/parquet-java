package parquet.thrift;

import parquet.schema.Type;
import parquet.thrift.projection.FieldsPath;

import static parquet.Preconditions.checkNotNull;

public class ConvertedField {
  private final Type type;
  private final FieldsPath path;
  private final boolean keep;

  private ConvertedField(Type type, FieldsPath path, boolean keep) {
    this.type = type;
    this.path = path;
    this.keep = keep;
  }

  public static ConvertedField keep(Type type, FieldsPath path) {
    return new ConvertedField(checkNotNull(type, "type"), checkNotNull(path, "path"), true);
  }

  public static ConvertedField drop(FieldsPath path) {
    return new ConvertedField(null, checkNotNull(path, "path"), false);
  }

  public Type getType() {
    if (!keep) {
      throw new IllegalArgumentException("getType called on a projected field");
    }
    return type;
  }

  public FieldsPath getPath() {
    return path;
  }

  public boolean keep() {
    return keep;
  }
}
