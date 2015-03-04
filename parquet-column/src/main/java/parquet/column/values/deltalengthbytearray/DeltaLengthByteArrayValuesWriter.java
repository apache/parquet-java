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
package parquet.column.values.deltalengthbytearray;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.bytes.LittleEndianDataOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;

/**
 * Write lengths of byte-arrays using delta encoding, followed by concatenated byte-arrays
 * <pre>
 *   {@code
 *   delta-length-byte-array : length* byte-array*
 *   }
 * </pre>
 * @author Aniket Mokashi
 *
 */
public class DeltaLengthByteArrayValuesWriter extends ValuesWriter {

  private static final Log LOG = Log.getLog(DeltaLengthByteArrayValuesWriter.class);

  private ValuesWriter lengthWriter;
  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;

  public DeltaLengthByteArrayValuesWriter(int initialSize, int pageSize) {
    arrayOut = new CapacityByteArrayOutputStream(initialSize, pageSize);
    out = new LittleEndianDataOutputStream(arrayOut);
    lengthWriter = new DeltaBinaryPackingValuesWriter(
        DeltaBinaryPackingValuesWriter.DEFAULT_NUM_BLOCK_VALUES,
        DeltaBinaryPackingValuesWriter.DEFAULT_NUM_MINIBLOCKS,
        initialSize, pageSize);
  }

  @Override
  public void writeBytes(Binary v) {
    try {
      lengthWriter.writeInteger(v.length());
      out.write(v.getBytes());
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write bytes", e);
    }
  }

  @Override
  public long getBufferedSize() {
    return lengthWriter.getBufferedSize() + arrayOut.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      out.flush();
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page", e);
    }
    if (Log.DEBUG) LOG.debug("writing a buffer of size " + arrayOut.size());
    return BytesInput.concat(lengthWriter.getBytes(), BytesInput.from(arrayOut));
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.DELTA_LENGTH_BYTE_ARRAY;
  }

  @Override
  public void reset() {
    lengthWriter.reset();
    arrayOut.reset();
  }

  @Override
  public long getAllocatedSize() {
    return lengthWriter.getAllocatedSize() + arrayOut.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return arrayOut.memUsageString(lengthWriter.memUsageString(prefix) + " DELTA_LENGTH_BYTE_ARRAY");
  }
}
