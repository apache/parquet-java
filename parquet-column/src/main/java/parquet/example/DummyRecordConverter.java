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
package parquet.example;

import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;

/**
 * Dummy implementation for perf tests
 *
 * @author Julien Le Dem
 *
 */
public final class DummyRecordConverter extends
    RecordConverter<Object> {
  Object a;

  public Object getCurrentRecord() {
    return a;
  }

  public GroupConverter getGroupConverter(int fieldIndex) {
    return new DummyRecordConverter();
  }

  public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
    return new PrimitiveConverter() {
      public void addBinary(byte[] value) {
        a = value;
      }
      public void addBoolean(boolean value) {
        a = value;
      }
      public void addDouble(double value) {
        a = value;
      }
      public void addFloat(float value) {
        a = value;
      }
      public void addInt(int value) {
        a = value;
      }
      public void addLong(long value) {
        a = value;
      }
    };
  }

  public void start() {
    a = "start()";
  }

  public void end() {
    a = "end()";
  }
}