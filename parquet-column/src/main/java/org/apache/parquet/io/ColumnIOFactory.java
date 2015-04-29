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
package org.apache.parquet.io;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.TypeVisitor;

/**
 * Factory constructing the ColumnIO structure from the schema
 *
 * @author Julien Le Dem
 *
 */
public class ColumnIOFactory {

  public class ColumnIOCreatorVisitor implements TypeVisitor {

    private MessageColumnIO columnIO;
    private GroupColumnIO current;
    private List<PrimitiveColumnIO> leaves = new ArrayList<PrimitiveColumnIO>();
    private final boolean validating;
    private final MessageType requestedSchema;
    private int currentRequestedIndex;
    private Type currentRequestedType;
    private boolean strictTypeChecking;

    public ColumnIOCreatorVisitor(boolean validating, MessageType requestedSchema) {
      this(validating, requestedSchema, true);
    }
    
    public ColumnIOCreatorVisitor(boolean validating, MessageType requestedSchema, boolean strictTypeChecking) {
      this.validating = validating;
      this.requestedSchema = requestedSchema;
      this.strictTypeChecking = strictTypeChecking;
    }

    @Override
    public void visit(MessageType messageType) {
      columnIO = new MessageColumnIO(requestedSchema, validating);
      visitChildren(columnIO, messageType, requestedSchema);
      columnIO.setLevels();
      columnIO.setLeaves(leaves);
    }

    @Override
    public void visit(GroupType groupType) {
      if (currentRequestedType.isPrimitive()) {
        incompatibleSchema(groupType, currentRequestedType);
      }
      GroupColumnIO newIO = new GroupColumnIO(groupType, current, currentRequestedIndex);
      current.add(newIO);
      visitChildren(newIO, groupType, currentRequestedType.asGroupType());
    }

    private void visitChildren(GroupColumnIO newIO, GroupType groupType, GroupType requestedGroupType) {
      GroupColumnIO oldIO = current;
      current = newIO;
      for (Type type : groupType.getFields()) {
        // if the file schema does not contain the field it will just stay null
        if (requestedGroupType.containsField(type.getName())) {
          currentRequestedIndex = requestedGroupType.getFieldIndex(type.getName());
          currentRequestedType = requestedGroupType.getType(currentRequestedIndex);
          if (currentRequestedType.getRepetition().isMoreRestrictiveThan(type.getRepetition())) {
            incompatibleSchema(type, currentRequestedType);
          }
          type.accept(this);
        }
      }
      current = oldIO;
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
      if (!currentRequestedType.isPrimitive() || 
              (this.strictTypeChecking && currentRequestedType.asPrimitiveType().getPrimitiveTypeName() != primitiveType.getPrimitiveTypeName())) {
        incompatibleSchema(primitiveType, currentRequestedType);
      }
      PrimitiveColumnIO newIO = new PrimitiveColumnIO(primitiveType, current, currentRequestedIndex, leaves.size());
      current.add(newIO);
      leaves.add(newIO);
    }

    private void incompatibleSchema(Type fileType, Type requestedType) {
      throw new ParquetDecodingException("The requested schema is not compatible with the file schema. incompatible types: " + requestedType + " != " + fileType);
    }

    public MessageColumnIO getColumnIO() {
      return columnIO;
    }

  }

  private final boolean validating;

  /**
   * validation is off by default
   */
  public ColumnIOFactory() {
    this(false);
  }

  /**
   * @param validating to turn validation on
   */
  public ColumnIOFactory(boolean validating) {
    super();
    this.validating = validating;
  }

  /**
   * @param schema the requestedSchema we want to read/write
   * @param fileSchema the file schema (when reading it can be different from the requested schema)
   * @return the corresponding serializing/deserializing structure
   */
  public MessageColumnIO getColumnIO(MessageType requestedSchema, MessageType fileSchema) {
    return getColumnIO(requestedSchema, fileSchema, true);
  }
  
  /**
   * @param schema the requestedSchema we want to read/write
   * @param fileSchema the file schema (when reading it can be different from the requested schema)
   * @param strict should file type and requested primitive types match
   * @return the corresponding serializing/deserializing structure
   */
  public MessageColumnIO getColumnIO(MessageType requestedSchema, MessageType fileSchema, boolean strict) {
    ColumnIOCreatorVisitor visitor = new ColumnIOCreatorVisitor(validating, requestedSchema, strict);
    fileSchema.accept(visitor);
    return visitor.getColumnIO();
  }

  /**
   * @param schema the schema we want to read/write
   * @return the corresponding serializing/deserializing structure
   */
  public MessageColumnIO getColumnIO(MessageType schema) {
    return this.getColumnIO(schema, schema);
  }

}
