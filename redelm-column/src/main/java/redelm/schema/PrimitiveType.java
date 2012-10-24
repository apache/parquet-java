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
package redelm.schema;

import redelm.column.ColumnReader;
import redelm.io.RecordConsumer;

public class PrimitiveType extends Type {
  public static enum Primitive {
    STRING {
      @Override
      public String toString(ColumnReader columnReader) {
        return columnReader.getString();
      }
      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addString(columnReader.getString());
      }
    },
    INT64 {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getLong());
      }
      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addLong(columnReader.getLong());
      }
    },
    INT32 {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getInt());
      }
      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addInt(columnReader.getInt());
      }
    },
    BOOL {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBool());
      }
      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBoolean(columnReader.getBool());
      }
    },
    BINARY {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBinary());
      }
      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBinary(columnReader.getBinary());
      }
    },
    FLOAT {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getFloat());
      }
      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addFloat(columnReader.getFloat());
      }
    },
    DOUBLE {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getDouble());
      }
      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addDouble(columnReader.getDouble());
      }
    };

    abstract public String toString(ColumnReader columnReader);

    abstract public void addValueToRecordConsumer(RecordConsumer recordConsumer, ColumnReader columnReader);

  }

  private final Primitive primitive;

  public PrimitiveType(Repetition repeatition, Primitive primitive, String name) {
    super(name, repeatition);
    this.primitive = primitive;
  }

  public Primitive getPrimitive() {
    return primitive;
  }

  @Override
  public boolean isPrimitive() {
    return true;
  }

  @Override
  public String toString() {
    return getName() + ": " + primitive;
  }

  @Override
  public String toString(String indent) {
    return indent + getRepetition().name().toLowerCase() + " " + primitive.name().toLowerCase() + " " + getName();
  }

  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

}
