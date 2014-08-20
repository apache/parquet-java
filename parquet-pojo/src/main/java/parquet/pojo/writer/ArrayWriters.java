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
  public static abstract class ArrayWriter<T> implements RecordWriter<T> {
    protected void writeLengthToConsumer(Object value, RecordConsumer recordConsumer) {
      int length = Array.getLength(value);
      recordConsumer.startField(null, 0);
      recordConsumer.addInteger(length);
      recordConsumer.endField(null, 0);
    }

    protected abstract void writeToConsumer(
      T array, RecordConsumer recordConsumer
    );

    @Override
    public void writeValue(T value, RecordConsumer recordConsumer) {
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

      writeValue((T)value, recordConsumer);

      recordConsumer.endGroup();
      recordConsumer.endField(null, index);

    }
  }

  public static class BoolArrayWriter extends ArrayWriter<boolean[]> {
    @Override
    protected void writeToConsumer(boolean[] array, RecordConsumer recordConsumer) {
      for(boolean b : array) {
        recordConsumer.addBoolean(b);
      }
    }
  }

  public static class CharArrayWriter extends ArrayWriter<char[]> {
    @Override
    protected void writeToConsumer(char[] array,  RecordConsumer recordConsumer) {
      for(char c : array) {
        recordConsumer.addInteger(c);
      }
    }
  }

  public static class ShortArrayWriter extends ArrayWriter<short[]> {
    @Override
    protected void writeToConsumer(short[] array, RecordConsumer recordConsumer) {
      for(short s : array) {
        recordConsumer.addInteger(s);
      }
    }
  }

  public static class IntArrayWriter extends ArrayWriter<int[]> {
    @Override
    protected void writeToConsumer(int[] array, RecordConsumer recordConsumer) {
      for(int i : array) {
        recordConsumer.addInteger(i);
      }
    }
  }

  public static class LongArrayWriter extends ArrayWriter<long[]> {
    @Override
    protected void writeToConsumer(long[] array, RecordConsumer recordConsumer) {
      for(long l : array) {
        recordConsumer.addLong(l);
      }
    }
  }

  public static class FloatArrayWriter extends ArrayWriter<float[]> {
    @Override
    protected void writeToConsumer(float[] array, RecordConsumer recordConsumer) {
      for(float f : array) {
        recordConsumer.addFloat(f);
      }
    }
  }

  public static class DoubleArrayWriter extends ArrayWriter<double[]> {
    @Override
    protected void writeToConsumer(double[] array, RecordConsumer recordConsumer) {
      for(double d : array) {
        recordConsumer.addDouble(d);
      }
    }
  }

  public static class ObjectArrayWriter<T> extends ArrayWriter<T[]> {
    private final RecordWriter<T> recordWriter;

    public ObjectArrayWriter(RecordWriter recordWriter) {
      this.recordWriter = recordWriter;
    }

    @Override
    protected void writeToConsumer(T[] array,  RecordConsumer recordConsumer) {
      for(T o : array) {
        recordWriter.writeValue(o, recordConsumer);
      }
    }

    @Override
    public void writeValue(T[] arr, RecordConsumer recordConsumer) {
      if (arr == null) {
        return;
      }

      writeLengthToConsumer(arr, recordConsumer);
      recordConsumer.startField(null, 1);
      for(T value : arr) {
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