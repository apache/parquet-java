package parquet.filter2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * This is a bit ugly, but it allows us to provide good error messages at runtime
 * when there are type mismatches.
 */
public final class ValidTypeMap {
  private ValidTypeMap() { }

  private static final Map<Class<?>, Set<FullTypeDescriptor>> classToParquetType = new HashMap<Class<?>, Set<FullTypeDescriptor>>();
  private static final Map<FullTypeDescriptor, Set<Class<?>>> parquetTypeToClass = new HashMap<FullTypeDescriptor, Set<Class<?>>>();

  private static void add(Class<?> c, FullTypeDescriptor f) {
    Set<FullTypeDescriptor> s = classToParquetType.get(c);
    if (s == null) {
      s = new HashSet<FullTypeDescriptor>();
      classToParquetType.put(c, s);
    }
    s.add(f);

    Set<Class<?>> i = parquetTypeToClass.get(f);
    if (i == null) {
      i = new HashSet<Class<?>>();
      parquetTypeToClass.put(f, i);
    }
    i.add(c);
  }

  static {
    add(Integer.class, new FullTypeDescriptor(PrimitiveTypeName.INT32, null));
    add(Long.class, new FullTypeDescriptor(PrimitiveTypeName.INT64, null));
    add(Float.class, new FullTypeDescriptor(PrimitiveTypeName.FLOAT, null));
    add(Double.class, new FullTypeDescriptor(PrimitiveTypeName.DOUBLE, null));
    add(String.class, new FullTypeDescriptor(PrimitiveTypeName.BINARY, OriginalType.UTF8));

    // these are both valid mappings
    add(byte[].class, new FullTypeDescriptor(PrimitiveTypeName.BINARY, null));
    add(byte[].class, new FullTypeDescriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, null));
  }

  public static void assertTypeValid(String columnPath, Class<?> foundColumnType, PrimitiveTypeName primitiveType, OriginalType originalType) {
    Set<FullTypeDescriptor> s = classToParquetType.get(foundColumnType);
    FullTypeDescriptor typeInFileMetaData = new FullTypeDescriptor(primitiveType, originalType);

    if (s == null) {
      StringBuilder message = new StringBuilder();
      message
          .append("Column ")
          .append(columnPath)
          .append(" is of type: ")
          .append(foundColumnType.getName())
          .append(" which is not supported in FilterPredicates.");

      Set<Class<?>> supportedTypes = parquetTypeToClass.get(typeInFileMetaData);
      if (supportedTypes != null) {
        message
          .append(" Supported types for this column are: ")
          .append(supportedTypes);
      }
      throw new IllegalArgumentException(message.toString());
    }

    if (!s.contains(typeInFileMetaData)) {
      StringBuilder message = new StringBuilder();
      message
          .append("FilterPredicate column: ")
          .append(columnPath)
          .append("'s type (")
          .append(foundColumnType.getName())
          .append(") does not match file metadata. Column ")
          .append(columnPath)
          .append(" is of type: ")
          .append(typeInFileMetaData)
          .append("\n Valid types for this column are: ")
          .append(parquetTypeToClass.get(typeInFileMetaData));
      throw new IllegalArgumentException(message.toString());
    }
  }

  private static final class FullTypeDescriptor {
    private final PrimitiveTypeName primitiveType;
    private final OriginalType originalType;

    private FullTypeDescriptor(PrimitiveTypeName primitiveType, OriginalType originalType) {
      this.primitiveType = primitiveType;
      this.originalType = originalType;
    }

    public PrimitiveTypeName getPrimitiveType() {
      return primitiveType;
    }

    public OriginalType getOriginalType() {
      return originalType;
    }

    @Override
    public String toString() {
      return "FullTypeDescriptor(" + "PrimitiveType: " + primitiveType + ", OriginalType: " + originalType + ')';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      FullTypeDescriptor that = (FullTypeDescriptor) o;

      if (originalType != that.originalType) return false;
      if (primitiveType != that.primitiveType) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = primitiveType != null ? primitiveType.hashCode() : 0;
      result = 31 * result + (originalType != null ? originalType.hashCode() : 0);
      return result;
    }
  }
}
