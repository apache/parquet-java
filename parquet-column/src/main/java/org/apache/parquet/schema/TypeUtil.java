/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.schema;

import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.FiniteVectorLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

public class TypeUtil {
  public static boolean checkValidFiniteVectorSchema(Type type) {
    boolean found = type.getLogicalTypeAnnotation() instanceof FiniteVectorLogicalTypeAnnotation;
    if (found) {
      if (type.isPrimitive()) {
        throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + type);
      }
      checkVector(type.asGroupType());
    } else if (!type.isPrimitive()) {
      for (Type child : type.asGroupType().getFields()) {
        found |= checkValidFiniteVectorSchema(child);
      }
    }
    return found;
  }

  public static boolean hasFiniteVector(Type type) {
    if (type.getLogicalTypeAnnotation() instanceof FiniteVectorLogicalTypeAnnotation) return true;
    if (!type.isPrimitive()) {
      for (Type child : type.asGroupType().getFields()) {
        if (hasFiniteVector(child)) return true;
      }
    }
    return false;
  }

  private static void checkVector(GroupType vector) {
    if (vector.getRepetition() == Repetition.REPEATED || vector.getFieldCount() != 1) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
    Type listType = vector.getType(0);
    if (listType.isPrimitive()
        || !listType.getName().equals("list")
        || listType.getRepetition() != Repetition.REPEATED
        || listType.getLogicalTypeAnnotation() != null) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
    GroupType list = listType.asGroupType();
    if (list.getFieldCount() != 1) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
    Type elementType = list.getType(0);
    if (!elementType.isPrimitive()
        || !elementType.getName().equals("element")
        || elementType.getRepetition() != Repetition.REQUIRED) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
    PrimitiveType element = elementType.asPrimitiveType();
    PrimitiveTypeName physical = element.getPrimitiveTypeName();
    LogicalTypeAnnotation logical = element.getLogicalTypeAnnotation();
    boolean allowed = switch (physical) {
      case BOOLEAN, FLOAT, DOUBLE -> logical == null;
      case INT32, INT64 ->
        logical == null
            || logical instanceof IntLogicalTypeAnnotation
            || logical instanceof DecimalLogicalTypeAnnotation;
      case FIXED_LEN_BYTE_ARRAY ->
        logical instanceof Float16LogicalTypeAnnotation || logical instanceof DecimalLogicalTypeAnnotation;
      default -> false;
    };
    if (!allowed) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
    try {
      Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive(physical, element.getRepetition());
      if (physical == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        builder.length(element.getTypeLength());
      }
      if (logical != null) {
        builder.as(logical);
      }
      builder.named(element.getName());
    } catch (IllegalArgumentException | IllegalStateException e) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector, e);
    }
  }

  public static FiniteVectorLogicalTypeAnnotation getFiniteVectorType(GroupType schema, String[] path) {
    if (path.length < 3) {
      return null;
    }
    GroupType group = schema;
    for (int i = 0; i < path.length - 2; i++) {
      group = group.getType(path[i]).asGroupType();
    }
    LogicalTypeAnnotation logical = group.getLogicalTypeAnnotation();
    return logical instanceof FiniteVectorLogicalTypeAnnotation
        ? (FiniteVectorLogicalTypeAnnotation) logical
        : null;
  }

  public static void checkValidWriteSchema(GroupType schema) {
    schema.accept(new TypeVisitor() {
      @Override
      public void visit(GroupType groupType) {
        if (groupType.getFieldCount() <= 0) {
          throw new InvalidSchemaException("Cannot write a schema with an empty group: " + groupType);
        }
        for (Type type : groupType.getFields()) {
          type.accept(this);
        }
      }

      @Override
      public void visit(MessageType messageType) {
        visit((GroupType) messageType);
      }

      @Override
      public void visit(PrimitiveType primitiveType) {}
    });
  }
}
