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
package org.apache.parquet.filter2.predicate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Contains all valid mappings from class -&gt; parquet type (and vice versa) for use in
 * {@link FilterPredicate}s
 * <p>
 * This is a bit ugly, but it allows us to provide good error messages at runtime
 * when there are type mismatches.
 * <p>
 * TODO: this has some overlap with {@link PrimitiveTypeName#javaType}
 * TODO: (https://github.com/apache/parquet-java/issues/1447)
 */
public class ValidTypeMap {
  private ValidTypeMap() {}

  // classToParquetType and parquetTypeToClass are used as a bi-directional map
  private static final Map<Class<?>, Set<PrimitiveTypeName>> classToParquetType = new HashMap<>();
  private static final Map<PrimitiveTypeName, Set<Class<?>>> parquetTypeToClass = new HashMap<>();

  // set up the mapping in both directions
  private static void add(Class<?> c, PrimitiveTypeName p) {
    Set<PrimitiveTypeName> descriptors = classToParquetType.get(c);
    if (descriptors == null) {
      descriptors = new HashSet<>();
      classToParquetType.put(c, descriptors);
    }
    descriptors.add(p);

    Set<Class<?>> classes = parquetTypeToClass.get(p);
    if (classes == null) {
      classes = new HashSet<>();
      parquetTypeToClass.put(p, classes);
    }
    classes.add(c);
  }

  static {
    for (PrimitiveTypeName t : PrimitiveTypeName.values()) {
      Class<?> c = t.javaType;

      if (c.isPrimitive()) {
        c = PrimitiveToBoxedClass.get(c);
      }

      add(c, t);
    }
  }

  /**
   * Asserts that foundColumn was declared as a type that is compatible with the type for this column found
   * in the schema of the parquet file.
   *
   * @param foundColumn   the column as declared by the user
   * @param primitiveType the primitive type according to the schema
   * @param <T>           the java Type of values in the column, must be Comparable
   * @throws java.lang.IllegalArgumentException if the types do not align
   */
  public static <T extends Comparable<T>> void assertTypeValid(
      Column<T> foundColumn, PrimitiveTypeName primitiveType) {
    Class<T> foundColumnType = foundColumn.getColumnType();
    ColumnPath columnPath = foundColumn.getColumnPath();

    Set<PrimitiveTypeName> validTypeDescriptors = classToParquetType.get(foundColumnType);

    if (validTypeDescriptors == null) {
      StringBuilder message = new StringBuilder();
      message.append("Column ")
          .append(columnPath.toDotString())
          .append(" was declared as type: ")
          .append(foundColumnType.getName())
          .append(" which is not supported in FilterPredicates.");

      Set<Class<?>> supportedTypes = parquetTypeToClass.get(primitiveType);
      if (supportedTypes != null) {
        message.append(" Supported types for this column are: ").append(supportedTypes);
      } else {
        message.append(" There are no supported types for columns of " + primitiveType);
      }
      throw new IllegalArgumentException(message.toString());
    }

    if (!validTypeDescriptors.contains(primitiveType)) {
      StringBuilder message = new StringBuilder();
      message.append("FilterPredicate column: ")
          .append(columnPath.toDotString())
          .append("'s declared type (")
          .append(foundColumnType.getName())
          .append(") does not match the schema found in file metadata. Column ")
          .append(columnPath.toDotString())
          .append(" is of type: ")
          .append(primitiveType)
          .append("\nValid types for this column are: ")
          .append(parquetTypeToClass.get(primitiveType));
      throw new IllegalArgumentException(message.toString());
    }
  }
}
