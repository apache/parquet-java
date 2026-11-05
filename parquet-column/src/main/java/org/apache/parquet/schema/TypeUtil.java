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

import org.apache.parquet.schema.LogicalTypeAnnotation.FiniteVectorLogicalTypeAnnotation;

public class TypeUtil {
  public static boolean hasFiniteVector(Type type) {
    if (type.getLogicalTypeAnnotation() instanceof FiniteVectorLogicalTypeAnnotation) return true;
    if (!type.isPrimitive()) {
      for (Type child : type.asGroupType().getFields()) {
        if (hasFiniteVector(child)) return true;
      }
    }
    return false;
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
