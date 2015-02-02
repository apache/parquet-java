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
package parquet.column.values.plain;

import static parquet.Log.DEBUG;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

public class BinaryPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(BinaryPlainValuesReader.class);
  private byte[] in;
  private int offset;

  @Override
  public Binary readBytes() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      int start = offset + 4;
      offset = start + length;
      return Binary.fromByteArray(in, start, length);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    }
  }

  @Override
  public void skip() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      offset += 4 + length;
    } catch (IOException e) {
      throw new ParquetDecodingException("could not skip bytes at offset " + offset, e);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not skip bytes at offset " + offset, e);
    }
  }

  @Override
  public void initFromPage(int valueCount, byte[] in, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = in;
    this.offset = offset;
  }

}
