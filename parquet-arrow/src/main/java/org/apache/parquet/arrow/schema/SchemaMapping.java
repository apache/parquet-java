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
package org.apache.parquet.arrow.schema;

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * The mapping between an Arrow and a Parquet schema
 * @see SchemaConverter
 *
 * @author Julien Le Dem
 */
public class SchemaMapping {

  private final Schema arrowSchema;
  private final MessageType parquetSchema;
  private final List<TypeMapping> children;

  SchemaMapping(Schema arrowSchema, MessageType parquetSchema, List<TypeMapping> children) {
    super();
    this.arrowSchema = arrowSchema;
    this.parquetSchema = parquetSchema;
    this.children = Collections.unmodifiableList(children);
  }

  public Schema getArrowSchema() {
    return arrowSchema;
  }

  public MessageType getParquetSchema() {
    return parquetSchema;
  }

  /**
   * @return mapping between individual fields of each of the 2 schemas (should be the same width)
   */
  public List<TypeMapping> getChildren() {
    return children;
  }

  /**
   * To traverse a schema mapping
   * @param <T>
   */
  public interface TypeMappingVisitor<T> {
    T visit(PrimitiveTypeMapping primitiveTypeMapping);
    T visit(StructTypeMapping structTypeMapping);
    T visit(UnionTypeMapping unionTypeMapping);
    T visit(ListTypeMapping listTypeMapping);
    T visit(RepeatedTypeMapping repeatedTypeMapping);
  }

  /**
   * Mapping between an Arrow and a Parquet types
   */
  public abstract static class TypeMapping {

    private final Field arrowField;
    private final Type parquetType;
    private List<TypeMapping> children;

    TypeMapping(Field arrowField, Type parquetType, List<TypeMapping> children) {
      super();
      this.arrowField = arrowField;
      this.parquetType = parquetType;
      this.children = children;
    }

    public Field getArrowField() {
      return arrowField;
    }

    public Type getParquetType() {
      return parquetType;
    }

    public List<TypeMapping> getChildren() {
      return children;
    }

    public abstract <T> T accept(TypeMappingVisitor<T> visitor);

  }

  /**
   * mapping between two primitive types
   */
  public static class PrimitiveTypeMapping extends TypeMapping {
    public PrimitiveTypeMapping(Field arrowField, PrimitiveType parquetType) {
      super(arrowField, parquetType, Collections.<TypeMapping>emptyList());
    }

    @Override
    public <T> T accept(TypeMappingVisitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * mapping of a struct type
   */
  public static class StructTypeMapping extends TypeMapping {
    public StructTypeMapping(Field arrowField, GroupType parquetType, List<TypeMapping> children) {
      super(arrowField, parquetType, children);
    }

    @Override
    public <T> T accept(TypeMappingVisitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * mapping of a union type
   */
  public static class UnionTypeMapping extends TypeMapping {
    public UnionTypeMapping(Field arrowField, GroupType parquetType, List<TypeMapping> children) {
      super(arrowField, parquetType, children);
    }

    @Override
    public <T> T accept(TypeMappingVisitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * mapping of a List type and standard 3-level List annotated Parquet type
   */
  public static class ListTypeMapping extends TypeMapping {
    private final List3Levels list3Levels;
    private final TypeMapping child;

    public ListTypeMapping(Field arrowField, List3Levels list3Levels, TypeMapping child) {
      super(arrowField, list3Levels.getList(), asList(child));
      this.list3Levels = list3Levels;
      this.child = child;
      if (list3Levels.getElement() != child.getParquetType()) {
        throw new IllegalArgumentException(list3Levels + " <=> " + child);
      }
    }

    public List3Levels getList3Levels() {
      return list3Levels;
    }

    public TypeMapping getChild() {
      return child;
    }

    @Override
    public <T> T accept(TypeMappingVisitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * mapping of a List type and repeated Parquet field (non-list annotated)
   */
  public static class RepeatedTypeMapping extends TypeMapping {
    private final TypeMapping child;

    public RepeatedTypeMapping(Field arrowField, Type parquetType, TypeMapping child) {
      super(arrowField, parquetType, asList(child));
      this.child = child;
    }

    public TypeMapping getChild() {
      return child;
    }

    @Override
    public <T> T accept(TypeMappingVisitor<T> visitor) {
      return visitor.visit(this);
    }
  }
}
