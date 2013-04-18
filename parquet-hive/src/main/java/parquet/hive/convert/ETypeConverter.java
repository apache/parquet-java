/**
 * Copyright 2013 Criteo.
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
package parquet.hive.convert;

import java.math.BigDecimal;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import parquet.Log;
import parquet.hive.writable.BigDecimalWritable;
import parquet.hive.writable.BinaryWritable;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.PrimitiveConverter;

/**
 *
 * TODO : doc + see classes below (duplicated code from julien)
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public enum ETypeConverter {

  EDOUBLE_CONVERTER(Double.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new FieldDoubleConverter(new ParentValueContainer() {
        @Override
        void add(final Object value) {
          LOG.info("adding value " + value + " at index " + index);
          parent.set(index, new DoubleWritable((Double) value));
        }
      });
    }
  },
  EBOOLEAN_CONVERTER(Boolean.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new FieldBooleanConverter(new ParentValueContainer() {
        @Override
        void add(final Object value) {
          LOG.info("adding value " + value + " at index " + index);
          parent.set(index, new BooleanWritable((Boolean) value));
        }
      });
    }
  },
  EFLOAT_CONVERTER(Float.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new FieldFloatConverter(new ParentValueContainer() {
        @Override
        void add(final Object value) {
          LOG.info("adding value " + value + " at index " + index);
          parent.set(index, new FloatWritable((Float) value));
        }
      });
    }
  },
  EINT32_CONVERTER(Integer.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new FieldIntegerConverter(new ParentValueContainer() {
        @Override
        void add(final Object value) {
          LOG.info("adding value " + value + " at index " + index);
          parent.set(index, new IntWritable((Integer) value));
        }
      });
    }
  },
  EINT64_CONVERTER(Long.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new FieldLongConverter(new ParentValueContainer() {
        @Override
        void add(final Object value) {
          LOG.info("adding value " + value + " at index " + index);
          parent.set(index, new LongWritable((Long) value));
        }
      });
    }
  },
  EINT96_CONVERTER(BigDecimal.class) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new FieldBigDecimalConverter(new ParentValueContainer() {
        @Override
        void add(final Object value) {
          LOG.info("adding value " + value + " at index " + index);
          parent.set(index, new BigDecimalWritable((BigDecimal) value));
        }
      });
    }
  },
  EBINARY_CONVERTER(Binary.class) {

    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new FieldBinaryConverter(new ParentValueContainer() {
        @Override
        void add(final Object value) {
          LOG.info("adding value " + value + " at index " + index);
          parent.set(index, new BinaryWritable((Binary) value));
        }
      });
    }

  };
  private static final Log LOG = Log.getLog(ETypeConverter.class);

  final Class<?> _type;

  private ETypeConverter(final Class<?> type) {
    this._type = type;
  }

  private Class<?> getType() {
    return _type;
  }

  abstract Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent);

  static public Converter getNewConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
    for (final ETypeConverter eConverter : values()) {
      if (eConverter.getType() == type) {
        return eConverter.getConverter(type, index, parent);
      }
    }
    throw new RuntimeException("Converter not found ... for type : " + type);
  }

  // TODO : Duplicate code with Julien, need to take a look after it works
  /**
   * handle string values
   *
   * @author Julien Le Dem
   *
   */
  final class FieldBinaryConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldBinaryConverter(final ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(final Binary value) {
      parent.add(value);
    }

  }

  /**
   * Handles doubles
   *
   * @author Julien Le Dem
   *
   */
  final class FieldDoubleConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldDoubleConverter(final ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addDouble(final double value) {
      parent.add(value);
    }

  }

  final class FieldIntegerConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldIntegerConverter(final ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addInt(final int value) {
      parent.add(value);
    }
  }

  final class FieldFloatConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldFloatConverter(final ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addFloat(final float value) {
      parent.add(value);
    }

  }

  final class FieldLongConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldLongConverter(final ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addLong(final long value) {
      parent.add(value);
    }

  }

  final class FieldBooleanConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldBooleanConverter(final ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addBoolean(final boolean value) {
      parent.add(value);
    }

  }

  // TODO NOT IMPLEMENTED YET !!!
  final class FieldBigDecimalConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldBigDecimalConverter(final ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addLong(final long value) {
      parent.add(value);
    }

  }

  /**
   * for converters to add their current value to their parent
   *
   * @author Julien Le Dem
   *
   */
  abstract public class ParentValueContainer {

    /**
     * will add the value to the parent whether it's a map, a bag or a tuple
     *
     * @param value
     */
    abstract void add(Object value);

  }

}
