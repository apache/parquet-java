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

import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.LittleEndianDataOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plain encoding except for booleans.
 *
 * <p>Endianness note for DECIMAL: when a DECIMAL value is stored in a
 * BYTE_ARRAY column, the {@link org.apache.parquet.io.api.Binary}
 * passed to this writer already contains the big-endian two's-complement bytes
 * of the un-scaled integer (the same bytes produced by
 * {@link java.math.BigInteger#toByteArray()}).  This writer keeps those bytes
 * exactly as they are and only adds the 4-byte little-endian length prefix
 * required by the PLAIN encoding.  Bytes are not re-ordered..</p>
 */
public class PlainValuesWriter extends ValuesWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PlainValuesWriter.class);

  @Deprecated
  public static final Charset CHARSET = Charset.forName("UTF-8");

  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;

  public PlainValuesWriter(int initialSize, int pageSize, ByteBufferAllocator allocator) {
    arrayOut = new CapacityByteArrayOutputStream(initialSize, pageSize, allocator);
    out = new LittleEndianDataOutputStream(arrayOut);
  }

  @Override
  public final void writeBytes(Binary v) {
    try {
      out.writeInt(v.length());
      v.writeTo(out);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write bytes", e);
    }
  }

  @Override
  public final void writeInteger(int v) {
    try {
      out.writeInt(v);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write int", e);
    }
  }

  @Override
  public final void writeLong(long v) {
    try {
      out.writeLong(v);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write long", e);
    }
  }

  @Override
  public final void writeFloat(float v) {
    try {
      out.writeFloat(v);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write float", e);
    }
  }

  @Override
  public final void writeDouble(double v) {
    try {
      out.writeDouble(v);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write double", e);
    }
  }

  @Override
  public void writeByte(int value) {
    try {
      out.write(value);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write byte", e);
    }
  }

  @Override
  public long getBufferedSize() {
    return arrayOut.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      out.flush();
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page", e);
    }
    if (LOG.isDebugEnabled()) LOG.debug("writing a buffer of size {}", arrayOut.size());
    return BytesInput.from(arrayOut);
  }

  @Override
  public void reset() {
    arrayOut.reset();
  }

  @Override
  public void close() {
    arrayOut.close();
    out.close();
  }

  @Override
  public long getAllocatedSize() {
    return arrayOut.getCapacity();
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.PLAIN;
  }

  @Override
  public String memUsageString(String prefix) {
    return arrayOut.memUsageString(prefix + " PLAIN");
  }
}
