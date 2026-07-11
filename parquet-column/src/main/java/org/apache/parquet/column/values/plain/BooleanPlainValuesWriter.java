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
package org.apache.parquet.column.values.plain;

import static org.apache.parquet.column.Encoding.PLAIN;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * An implementation of the PLAIN encoding for BOOLEAN values.
 *
 * <p>Packs booleans directly into bytes (8 per byte, LSB first) without
 * going through the generic int-based bit-packing encoder.
 */
public class BooleanPlainValuesWriter extends ValuesWriter {

  private static final int INITIAL_SLAB_SIZE = 1024;
  private static final int MAX_CAPACITY = 64 * 1024;

  private final CapacityByteArrayOutputStream baos;
  private int currentByte;
  private int bitsWritten;

  public BooleanPlainValuesWriter() {
    this.baos = new CapacityByteArrayOutputStream(INITIAL_SLAB_SIZE, MAX_CAPACITY);
    this.currentByte = 0;
    this.bitsWritten = 0;
  }

  @Override
  public final void writeBoolean(boolean v) {
    currentByte |= ((v ? 1 : 0) << bitsWritten);
    bitsWritten++;
    if (bitsWritten == 8) {
      baos.write(currentByte);
      currentByte = 0;
      bitsWritten = 0;
    }
  }

  @Override
  public long getBufferedSize() {
    return baos.size() + (bitsWritten > 0 ? 1 : 0);
  }

  @Override
  public BytesInput getBytes() {
    if (bitsWritten > 0) {
      baos.write(currentByte);
      currentByte = 0;
      bitsWritten = 0;
    }
    return BytesInput.from(baos);
  }

  @Override
  public void reset() {
    baos.reset();
    currentByte = 0;
    bitsWritten = 0;
  }

  @Override
  public void close() {
    baos.close();
  }

  @Override
  public long getAllocatedSize() {
    return baos.getCapacity();
  }

  @Override
  public Encoding getEncoding() {
    return PLAIN;
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s BooleanPlainValuesWriter %d bytes", prefix, getAllocatedSize());
  }
}
