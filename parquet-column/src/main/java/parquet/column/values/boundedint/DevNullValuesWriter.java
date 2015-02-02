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
package parquet.column.values.boundedint;

import static parquet.column.Encoding.BIT_PACKED;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.api.Binary;

/**
 * This is a special writer that doesn't write anything. The idea being that
 * some columns will always be the same value, and this will capture that. An
 * example is the set of repetition levels for a schema with no repeated fields.
 */
public class DevNullValuesWriter extends ValuesWriter {
  @Override
  public long getBufferedSize() {
    return 0;
  }

  @Override
  public void reset() {
  }

  @Override
  public void writeInteger(int v) {
  }

  @Override
  public void writeByte(int value) {
  }

  @Override
  public void writeBoolean(boolean v) {
  }

  @Override
  public void writeBytes(Binary v) {
  }

  @Override
  public void writeLong(long v) {
  }

  @Override
  public void writeDouble(double v) {
  }

  @Override
  public void writeFloat(float v) {
  }

  @Override
  public BytesInput getBytes() {
    return BytesInput.empty();
  }

  @Override
  public long getAllocatedSize() {
    return 0;
  }

  @Override
  public Encoding getEncoding() {
    return BIT_PACKED;
  }

  @Override
  public String memUsageString(String prefix) {
    return prefix + "0";
  }
}