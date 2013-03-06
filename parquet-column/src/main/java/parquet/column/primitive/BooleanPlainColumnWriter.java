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

import static parquet.column.Encoding.PLAIN;
import static parquet.column.primitive.BitPacking.getBitPackingWriter;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.primitive.BitPacking.BitPackingWriter;
import parquet.io.ParquetEncodingException;


/**
 * An implementation of the PLAIN encoding
 *
 * @author Julien Le Dem
 *
 */
public class BooleanPlainColumnWriter extends DataColumnWriter {
  private static final Log LOG = Log.getLog(BooleanPlainColumnWriter.class);

  private CapacityByteArrayOutputStream out;
  private BitPackingWriter bitPackingWriter;

  public BooleanPlainColumnWriter(int initialSize) {
    out = new CapacityByteArrayOutputStream(initialSize);
    bitPackingWriter = getBitPackingWriter(1, out);
  }

  @Override
  public final void writeBoolean(boolean v) {
    try {
      bitPackingWriter.write(v ? 1 : 0);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write boolean", e);
    }
  }

  @Override
  public long getBufferedSize() {
    return out.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      bitPackingWriter.finish();
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page", e);
    }
    if (Log.DEBUG) LOG.debug("writing a buffer of size " + out.size());
    return BytesInput.from(out);
  }

  @Override
  public void reset() {
    out.reset();
    bitPackingWriter = getBitPackingWriter(1, out);
  }

  @Override
  public long getAllocatedSize() {
    return out.getCapacity();
  }

  @Override
  public Encoding getEncoding() {
    return PLAIN;
  }

}
