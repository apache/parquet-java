/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.filter2.predicate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import parquet.common.schema.ColumnPath;
import parquet.filter2.predicate.Operators.Column;
import parquet.io.api.Binary;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Contains all valid mappings from class -> parquet type (and vice versa) for use in
 * {@link FilterPredicate}s
 *
 * This is a bit ugly, but it allows us to provide good error messages at runtime
 * when there are type mismatches.
 *
 * TODO: this has some overlap with {@link PrimitiveTypeName#javaType}
 * TODO: (https://issues.apache.org/jira/browse/PARQUET-30)
 */
public class ValidTypeMap {
  private ValidTypeMap() { }

  // classToParquetType and parquetTypeToClass are used as a bi-directional map
  private static final Map<Class<?>, Set<FullTypeDescriptor>> classToParquetType = new HashMap<Class<?>, Set<FullTypeDescriptor>>();
  private static final Map<FullTypeDescriptor, Set<Class<?>>> parquetTypeToClass = new HashMap<FullTypeDescriptor, Set<Class<?>>>();

  // set up the mapping in both directions
  private static void add(Class<?> c, FullTypeDescriptor f) {
    Set<FullTypeDescriptor> descriptors = classToParquetType.get(c);
    if (descriptors == null) {
      descriptors = new HashSet<FullTypeDescriptor>();
      classToParquetType.put(c, descriptors);
    }
    descriptors.add(f);

    Set<Class<?>> classes = parquetTypeToClass.get(f);
    if (classes == null) {
      classes = new HashSet<Class<?>>();
      parquetTypeToClass.put(f, classes);
    }
    classes.add(c);
  }

  static {
    // basic primitive columns
    add(Integer.class, new FullTypeDescriptor(PrimitiveTypeName.INT32, null));
    add(Long.class, new FullTypeDescriptor(PrimitiveTypeName.INT64, null));
    add(Float.class, new FullTypeDescriptor(PrimitiveTypeName.FLOAT, null));
    add(Double.class, new FullTypeDescriptor(PrimitiveTypeName.DOUBLE, null));
    add(Boolean.class, new FullTypeDescriptor(PrimitiveTypeName.BOOLEAN, null));

    // Both of these binary types are valid
    add(Binary.class, new FullTypeDescriptor(PrimitiveTypeName.BINARY, null));
    add(Binary.class, new FullTypeDescriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, null));

    add(Binary.class, new FullTypeDescriptor(PrimitiveTypeName.BINARY, OriginalType.UTF8));
    add(Binary.class, new FullTypeDescriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, OriginalType.UTF8));
  }

  /**
   * Asserts that foundColumn was declared as a type that is compatible with the type for this column found
   * in the schema of the parquet file.
   *
   * @throws java.lang.IllegalArgumentException if the types do not align
   *
   * @param foundColumn the column as declared by the user
   * @param primitiveType the primitive type according to the schema
   * @param originalType the original type according to the schema
   */
  public static <T extends Comparable<T>> void assertTypeValid(Column<T> foundColumn, PrimitiveTypeName primitiveType, OriginalType originalType) {
    Class<T> foundColumnType = foundColumn.getColumnType();
    ColumnPath columnPath = foundColumn.getColumnPath();

    Set<FullTypeDescriptor> validTypeDescriptors = classToParquetType.get(foundColumnType);
    FullTypeDescriptor typeInFileMetaData = new FullTypeDescriptor(primitiveType, originalType);

    if (validTypeDescriptors == null) {
      StringBuilder message = new StringBuilder();
      message
          .append("Column ")
          .append(columnPath.toDotString())
          .append(" was declared as type: ")
          .append(foundColumnType.getName())
          .append(" which is not supported in FilterPredicates.");

      Set<Class<?>> supportedTypes = parquetTypeToClass.get(typeInFileMetaData);
      if (supportedTypes != null) {
        message
          .append(" Supported types for this column are: ")
          .append(supportedTypes);
      } else {
        message.append(" There are no supported types for columns of " + typeInFileMetaData);
      }
      throw new IllegalArgumentException(message.toString());
    }

    if (!validTypeDescriptors.contains(typeInFileMetaData)) {
      StringBuilder message = new StringBuilder();
      message
          .append("FilterPredicate column: ")
          .append(columnPath.toDotString())
          .append("'s declared type (")
          .append(foundColumnType.getName())
          .append(") does not match the schema found in file metadata. Column ")
          .append(columnPath.toDotString())
          .append(" is of type: ")
          .append(typeInFileMetaData)
          .append("\nValid types for this column are: ")
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
