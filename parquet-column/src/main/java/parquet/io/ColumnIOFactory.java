/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.io;

import static parquet.schema.Type.Repetition.REQUIRED;

import java.util.ArrayList;
import java.util.List;

import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.TypeVisitor;
import parquet.schema.Type.Repetition;

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
    private final MessageType fileSchema;
    private int currentFileIndex;
    private Type currentFileType;

    public ColumnIOCreatorVisitor(boolean validating, MessageType fileSchema) {
      this.validating = validating;
      this.fileSchema = fileSchema;
    }

    @Override
    public void visit(MessageType messageType) {
      columnIO = new MessageColumnIO(messageType, validating);
      visitChildren(columnIO, messageType, fileSchema);
      columnIO.setLevels();
      columnIO.setLeaves(leaves);
    }

    @Override
    public void visit(GroupType groupType) {
      if (currentFileType.isPrimitive()) {
        incompatibleSchema(groupType, currentFileType);
      }
      GroupColumnIO newIO = new GroupColumnIO(groupType, current, currentFileIndex);
      current.add(newIO);
      visitChildren(newIO, groupType, currentFileType.asGroupType());
    }

    private void visitChildren(GroupColumnIO newIO, GroupType groupType, GroupType fileGroupType) {
      GroupColumnIO oldIO = current;
      current = newIO;
      for (Type type : groupType.getFields()) {
        // if the file schema does not contain the field it will just stay null
        if (fileGroupType.containsField(type.getName())) {
          currentFileIndex = fileGroupType.getFieldIndex(type.getName());
          currentFileType = fileGroupType.getType(currentFileIndex);
          if (type.getRepetition().isMoreRestrictiveThan(currentFileType.getRepetition())) {
            incompatibleSchema(type, currentFileType);
          }
          type.accept(this);
        } else if (type.getRepetition() == REQUIRED) {
          // if the missing field is required we fail
          // TODO: add support for default values
          throw new ParquetDecodingException("The requested schema is not compatible with the file schema. Missing required field in file " + type);
        }
      }
      current = oldIO;
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
      if (!currentFileType.isPrimitive() || currentFileType.asPrimitiveType().getPrimitiveTypeName() != primitiveType.getPrimitiveTypeName()) {
        incompatibleSchema(primitiveType, currentFileType);
      }
      PrimitiveColumnIO newIO = new PrimitiveColumnIO(primitiveType, current, currentFileIndex, leaves.size());
      current.add(newIO);
      leaves.add(newIO);
    }

    private void incompatibleSchema(Type requestedType, Type fileType) {
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
    ColumnIOCreatorVisitor visitor = new ColumnIOCreatorVisitor(validating, fileSchema);
    requestedSchema.accept(visitor);
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
