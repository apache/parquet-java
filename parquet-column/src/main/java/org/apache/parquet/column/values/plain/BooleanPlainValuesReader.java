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

import static org.apache.parquet.column.values.bitpacking.Packer.LITTLE_ENDIAN;

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * encodes boolean for the plain encoding: one bit at a time (0 = false)
 */
public class BooleanPlainValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(BooleanPlainValuesReader.class);

  private ByteBitPackingValuesReader in = new ByteBitPackingValuesReader(1, LITTLE_ENDIAN);

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.values.ValuesReader#readBoolean()
   */
  @Override
  public boolean readBoolean() {
    return in.readInteger() == 0 ? false : true;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.values.ValuesReader#skip()
   */
  @Override
  public void skip() {
    in.readInteger();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.values.ValuesReader#initFromPage(int, ByteBufferInputStream)
   */
  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());
    this.in.initFromPage(valueCount, stream);
  }

  @Deprecated
  @Override
  public int getNextOffset() {
    return in.getNextOffset();
  }
}
