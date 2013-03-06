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
package parquet.column.primitive;

import java.io.IOException;
import java.nio.charset.Charset;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.bytes.LittleEndianDataOutputStream;
import parquet.column.Encoding;
import parquet.io.Binary;
import parquet.io.ParquetEncodingException;


/**
 * Plain encoding except for booleans
 *
 * @author Julien Le Dem
 *
 */
public class PlainColumnWriter extends DataColumnWriter {
  private static final Log LOG = Log.getLog(PlainColumnWriter.class);

  public static final Charset CHARSET = Charset.forName("UTF-8");

  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;

  public PlainColumnWriter(int initialSize) {
    arrayOut = new CapacityByteArrayOutputStream(initialSize);
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
    if (Log.DEBUG) LOG.debug("writing a buffer of size " + arrayOut.size());
    return BytesInput.from(arrayOut);
  }

  @Override
  public void reset() {
    arrayOut.reset();
  }

  @Override
  public long getAllocatedSize() {
    return arrayOut.getCapacity();
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.PLAIN;
  }

}
