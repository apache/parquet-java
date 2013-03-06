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
package parquet.schema;

import java.util.Arrays;
import java.util.List;

import parquet.column.ColumnReader;
import parquet.io.InvalidRecordException;
import parquet.io.RecordConsumer;
import parquet.io.convert.PrimitiveConverter;


/**
 *
 * Representation of a Primitive type
 *
 * @author Julien Le Dem
 *
 */
public class PrimitiveType extends Type {

  /**
   * Supported Primitive types
   *
   * @author Julien Le Dem
   */
  public static enum PrimitiveTypeName {
      INT64("getLong", Long.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getLong());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addLong(columnReader.getLong());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addLong(columnReader.getLong());
      }
    },
    INT32("getInteger", Integer.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getInteger());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addInteger(columnReader.getInteger());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addInt(columnReader.getInteger());
      }
    },
    BOOLEAN("getBoolean", Boolean.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBoolean());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBoolean(columnReader.getBoolean());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addBoolean(columnReader.getBoolean());
      }
    },
    // TODO: array type literal?
    BINARY("getBinary", new byte[]{}.getClass()) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBinary());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBinary(columnReader.getBinary());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addBinary(columnReader.getBinary());
      }
    },
    FLOAT("getFloat", Float.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getFloat());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addFloat(columnReader.getFloat());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addFloat(columnReader.getFloat());
      }
    },
    DOUBLE("getDouble", Double.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getDouble());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addDouble(columnReader.getDouble());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addDouble(columnReader.getDouble());
      }
    };

    public final String getMethod;
    public final Class<?> javaType;

    private PrimitiveTypeName(String getMethod, Class<?> javaType) {
      this.getMethod = getMethod;
      this.javaType = javaType;
    }

    /**
     * reads the value from the columnReader with the appropriate accessor and returns a String representation
     * @param columnReader
     * @return a string
     */
    abstract public String toString(ColumnReader columnReader);

    /**
     * reads the value from the columnReader with the appropriate accessor and writes it to the recordConsumer
     * @param recordConsumer where to write
     * @param columnReader where to read from
     */
    abstract public void addValueToRecordConsumer(RecordConsumer recordConsumer,
        ColumnReader columnReader);

    abstract public void addValueToPrimitiveConverter(
        PrimitiveConverter primitiveConverter, ColumnReader columnReader);

  }

  private final PrimitiveTypeName primitive;

  /**
   *
   * @param repetition the OPTIONAL, REPEATED, REQUIRED
   * @param primitive STRING, INT64, ...
   * @param name the name of the type
   */
  public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive, String name) {
    this(repetition, primitive, name, null);
  }

  public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive, String name, OriginalType originalType) {
    super(name, repetition, originalType);
    this.primitive = primitive;
  }

  /**
   * @return the primitive type
   */
  public PrimitiveTypeName getPrimitiveTypeName() {
    return primitive;
  }

  /**
   * @return true
   */
  @Override
  public boolean isPrimitive() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeToStringBuilder(StringBuilder sb, String indent) {
    sb.append(indent)
        .append(getRepetition().name().toLowerCase())
        .append(" ")
        .append(primitive.name().toLowerCase())
        .append(" ")
        .append(getName());
    if (getOriginalType() != null) {
      sb.append(" (").append(getOriginalType()).append(")");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean typeEquals(Type other) {
    if (other.isPrimitive()) {
      PrimitiveType primitiveType = other.asPrimitiveType();
      return getRepetition() == primitiveType.getRepetition() &&
          getPrimitiveTypeName().equals(primitiveType.getPrimitiveTypeName()) &&
          getName().equals(primitiveType.getName());
    } else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected int typeHashCode() {
    int hash = 17;
    hash += 31 * getRepetition().hashCode();
    hash += 31 * getPrimitiveTypeName().hashCode();
    hash += 31 * getName().hashCode();
    return hash;
  }

  @Override
  public int getMaxRepetitionLevel(String[] path, int i) {
    if (path.length != i) {
      throw new InvalidRecordException("Arrived at primitive node, path invalid");
    }
    return getRepetition() == Repetition.REPEATED ? 1 : 0;
  }

  @Override
  public int getMaxDefinitionLevel(String[] path, int i) {
    if (path.length != i) {
      throw new InvalidRecordException("Arrived at primitive node, path invalid");
    }
    return getRepetition() == Repetition.REQUIRED ? 0 : 1;
  }

  @Override
  public Type getType(String[] path, int i) {
    if (path.length != i) {
      throw new InvalidRecordException("Arrived at primitive node at index " + i + " , path invalid: " + Arrays.toString(path));
    }
    return this;
  }

  @Override
  protected List<String[]> getPaths(int depth) {
    return Arrays.<String[]>asList(new String[depth]);
  }

  @Override
  void checkContains(Type subType) {
    super.checkContains(subType);
    if (!subType.isPrimitive()) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
    PrimitiveType primitiveType = subType.asPrimitiveType();
    if (this.primitive != primitiveType.primitive) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }

  }

  @Override
  public <T> T convert(List<GroupType> path, TypeConverter<T> converter) {
    return converter.convertPrimitiveType(path, this);
  }
}
