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
package org.apache.parquet.avro;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;

abstract class ParentValueContainer {

  /**
   * Adds the value to the parent.
   */
  public void add(Object value) {
    throw new RuntimeException("[BUG] ParentValueContainer#add was not overridden");
  }

  public void addBoolean(boolean value) {
    add(value);
  }

  public void addByte(byte value) {
    add(value);
  }

  public void addChar(char value) {
    add(value);
  }

  public void addShort(short value) {
    add(value);
  }

  public void addInt(int value) {
    add(value);
  }

  public void addLong(long value) {
    add(value);
  }

  public void addFloat(float value) {
    add(value);
  }

  public void addDouble(double value) {
    add(value);
  }

  static class LogicalTypePrimitiveContainer extends ParentValueContainer {
    private final ParentValueContainer wrapped;
    private final Schema schema;
    private final LogicalType logicalType;
    private final Conversion conversion;

    public LogicalTypePrimitiveContainer(ParentValueContainer wrapped, Schema schema, Conversion conversion) {
      this.wrapped = wrapped;
      this.schema = schema;
      this.logicalType = schema.getLogicalType();
      this.conversion = conversion;
    }

    @Override
    public void addDouble(double value) {
      wrapped.add(conversion.fromDouble(value, schema, logicalType));
    }

    @Override
    public void addFloat(float value) {
      wrapped.add(conversion.fromFloat(value, schema, logicalType));
    }

    @Override
    public void addLong(long value) {
      wrapped.add(conversion.fromLong(value, schema, logicalType));
    }

    @Override
    public void addInt(int value) {
      wrapped.add(conversion.fromInt(value, schema, logicalType));
    }

    @Override
    public void addShort(short value) {
      wrapped.add(conversion.fromInt((int) value, schema, logicalType));
    }

    @Override
    public void addChar(char value) {
      wrapped.add(conversion.fromInt((int) value, schema, logicalType));
    }

    @Override
    public void addByte(byte value) {
      wrapped.add(conversion.fromInt((int) value, schema, logicalType));
    }

    @Override
    public void addBoolean(boolean value) {
      wrapped.add(conversion.fromBoolean(value, schema, logicalType));
    }
  }

  static ParentValueContainer getConversionContainer(
      final ParentValueContainer parent, final Conversion<?> conversion, final Schema schema) {
    if (conversion == null) {
      return parent;
    }

    final LogicalType logicalType = schema.getLogicalType();

    switch (schema.getType()) {
      case STRING:
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromCharSequence((CharSequence) value, schema, logicalType));
          }
        };
      case BOOLEAN:
        return new LogicalTypePrimitiveContainer(parent, schema, conversion) {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromBoolean((Boolean) value, schema, logicalType));
          }
        };
      case INT:
        return new LogicalTypePrimitiveContainer(parent, schema, conversion) {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromInt((Integer) value, schema, logicalType));
          }
        };
      case LONG:
        return new LogicalTypePrimitiveContainer(parent, schema, conversion) {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromLong((Long) value, schema, logicalType));
          }
        };
      case FLOAT:
        return new LogicalTypePrimitiveContainer(parent, schema, conversion) {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromFloat((Float) value, schema, logicalType));
          }
        };
      case DOUBLE:
        return new LogicalTypePrimitiveContainer(parent, schema, conversion) {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromDouble((Double) value, schema, logicalType));
          }
        };
      case BYTES:
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromBytes((ByteBuffer) value, schema, logicalType));
          }
        };
      case FIXED:
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromFixed((GenericData.Fixed) value, schema, logicalType));
          }
        };
      case RECORD:
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromRecord((IndexedRecord) value, schema, logicalType));
          }
        };
      case ARRAY:
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromArray((Collection<?>) value, schema, logicalType));
          }
        };
      case MAP:
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromMap((Map<?, ?>) value, schema, logicalType));
          }
        };
      case ENUM:
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            parent.add(conversion.fromEnumSymbol((GenericEnumSymbol) value, schema, logicalType));
          }
        };
      default:
        return new LogicalTypePrimitiveContainer(parent, schema, conversion);
    }
  }
}
