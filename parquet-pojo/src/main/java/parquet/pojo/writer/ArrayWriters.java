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
package parquet.pojo.writer;

import parquet.io.api.RecordConsumer;
import parquet.pojo.field.FieldAccessor;

import java.lang.reflect.Array;

public class ArrayWriters {
  /**
   * Base class for writing array objects to {@link RecordConsumer}. Writes lengths as integers, then writes the repeated values
   */
  public static abstract class ArrayWriter implements RecordWriter {
    protected void writeLengthToConsumer(Object value, RecordConsumer recordConsumer) {
      int length = Array.getLength(value);
      recordConsumer.startField(null, 0);
      recordConsumer.addInteger(length);
      recordConsumer.endField(null, 0);
    }

    protected abstract void writeToConsumer(
      Object array, RecordConsumer recordConsumer
    );

    @Override
    public void writeValue(Object value, RecordConsumer recordConsumer) {
      if (value == null) {
        return;
      }

      writeLengthToConsumer(value, recordConsumer);
      recordConsumer.startField(null, 1);
      writeToConsumer(value, recordConsumer);
      recordConsumer.endField(null, 1);
    }

    @Override
    public void writeFromField(
      Object parent, RecordConsumer recordConsumer, int index, FieldAccessor fieldAccessor
    ) {
      Object value = fieldAccessor.get(parent);

      if (value == null) {
        return;
      }

      recordConsumer.startField(null, index);
      recordConsumer.startGroup();

      writeValue(value, recordConsumer);

      recordConsumer.endGroup();
      recordConsumer.endField(null, index);

    }
  }

  public static class BoolArrayWriter extends ArrayWriter {
    @Override
    protected void writeToConsumer(Object array, RecordConsumer recordConsumer) {
      boolean[] values = (boolean[]) array;

      for(boolean b : values) {
        recordConsumer.addBoolean(b);
      }
    }
  }

  public static class CharArrayWriter extends ArrayWriter {
    @Override
    protected void writeToConsumer(Object array,  RecordConsumer recordConsumer) {
      char[] chars = (char[]) array;
      for(char c : chars) {
        recordConsumer.addInteger(c);
      }
    }
  }

  public static class ShortArrayWriter extends ArrayWriter {
    @Override
    protected void writeToConsumer(Object array, RecordConsumer recordConsumer) {
      short[] shorts = (short[]) array;
      for(short s : shorts) {
        recordConsumer.addInteger(s);
      }
    }
  }

  public static class IntArrayWriter extends ArrayWriter {
    @Override
    protected void writeToConsumer(Object array, RecordConsumer recordConsumer) {
      int[] ints = (int[]) array;
      for(int i : ints) {
        recordConsumer.addInteger(i);
      }
    }
  }

  public static class LongArrayWriter extends ArrayWriter {
    @Override
    protected void writeToConsumer(Object array, RecordConsumer recordConsumer) {
      long[] longs = (long[]) array;
      for(long l : longs) {
        recordConsumer.addLong(l);
      }
    }
  }

  public static class FloatArrayWriter extends ArrayWriter {
    @Override
    protected void writeToConsumer(Object array, RecordConsumer recordConsumer) {
      float[] floats = (float[]) array;
      for(float f : floats) {
        recordConsumer.addFloat(f);
      }
    }
  }

  public static class DoubleArrayWriter extends ArrayWriter {
    @Override
    protected void writeToConsumer(Object array, RecordConsumer recordConsumer) {
      double[] doubles = (double[]) array;
      for(double d : doubles) {
        recordConsumer.addDouble(d);
      }
    }
  }

  public static class ObjectArrayWriter extends ArrayWriter {
    private final RecordWriter recordWriter;

    public ObjectArrayWriter(RecordWriter recordWriter) {
      this.recordWriter = recordWriter;
    }

    @Override
    protected void writeToConsumer(Object array,  RecordConsumer recordConsumer) {
      Object[] objects = (Object[]) array;
      for(Object o : objects) {
        recordWriter.writeValue(o, recordConsumer);
      }
    }

    @Override
    public void writeValue(Object array, RecordConsumer recordConsumer) {
      if (array == null) {
        return;
      }

      writeLengthToConsumer(array, recordConsumer);
      Object[] arr = (Object[]) array;
      recordConsumer.startField(null, 1);
      for (int i = 0; i < arr.length; i++) {
        Object value = arr[i];

        if (value != null) {
          recordConsumer.startGroup();
          recordConsumer.startField(null, 0);
          recordWriter.writeValue(value, recordConsumer);
          recordConsumer.endField(null, 0);
          recordConsumer.endGroup();
        }
      }
      recordConsumer.endField(null, 1);
    }
  }
}