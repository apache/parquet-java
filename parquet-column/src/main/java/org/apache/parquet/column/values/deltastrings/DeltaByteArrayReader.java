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
package org.apache.parquet.column.values.deltastrings;

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.io.api.Binary;

/**
 * Reads binary data written by {@link DeltaByteArrayWriter}
 */
public class DeltaByteArrayReader extends ValuesReader implements RequiresPreviousReader {
  private ValuesReader prefixLengthReader;
  private ValuesReader suffixReader;

  private Binary previous;

  public DeltaByteArrayReader() {
    this.prefixLengthReader = new DeltaBinaryPackingValuesReader();
    this.suffixReader = new DeltaLengthByteArrayValuesReader();
    this.previous = Binary.fromConstantByteArray(new byte[0]);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream)
      throws IOException {
    prefixLengthReader.initFromPage(valueCount, stream);
    suffixReader.initFromPage(valueCount, stream);
  }

  @Override
  public void skip() {
    // read the next value to skip so that previous is correct.
    readBytes();
  }

  @Override
  public Binary readBytes() {
    int prefixLength = prefixLengthReader.readInteger();
    // This does not copy bytes
    Binary suffix = suffixReader.readBytes();
    int length = prefixLength + suffix.length();

    // NOTE: due to PARQUET-246, it is important that we
    // respect prefixLength which was read from prefixLengthReader,
    // even for the *first* value of a page. Even though the first
    // value of the page should have an empty prefix, it may not
    // because of PARQUET-246.

    // We have to do this to materialize the output
    if(prefixLength != 0) {
      byte[] out = new byte[length];
      System.arraycopy(previous.getBytesUnsafe(), 0, out, 0, prefixLength);
      System.arraycopy(suffix.getBytesUnsafe(), 0, out, prefixLength, suffix.length());
      previous =  Binary.fromConstantByteArray(out);
    } else {
      previous = suffix;
    }
    return previous;
  }

  /**
   * There was a bug (PARQUET-246) in which DeltaByteArrayWriter's reset() method did not
   * clear the previous value state that it tracks internally. This resulted in the first
   * value of all pages (except for the first page) to be a delta from the last value of the
   * previous page. In order to read corrupted files written with this bug, when reading a
   * new page we need to recover the previous page's last value to use it (if needed) to
   * read the first value.
   */
  @Override
  public void setPreviousReader(ValuesReader reader) {
    if (reader != null) {
      this.previous = ((DeltaByteArrayReader) reader).previous;
    }
  }
}
