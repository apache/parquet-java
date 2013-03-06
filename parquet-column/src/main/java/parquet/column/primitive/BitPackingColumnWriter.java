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

import static parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static parquet.column.primitive.BitPacking.getBitPackingWriter;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.primitive.BitPacking.BitPackingWriter;
import parquet.io.ParquetEncodingException;

/**
 * a column writer that packs the ints in the number of bits required based on the maximum size.
 *
 * @author Julien Le Dem
 *
 */
public class BitPackingColumnWriter extends PrimitiveColumnWriter {

  private CapacityByteArrayOutputStream out;
  private BitPackingWriter bitPackingWriter;
  private int bitsPerValue;

  /**
   *
   * @param bound the maximum value stored by this column
   */
  public BitPackingColumnWriter(int bound) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
    this.out = new CapacityByteArrayOutputStream(32*1024); // size needed could be small but starting at 32 is really small
    init();
  }

  private void init() {
    this.bitPackingWriter = getBitPackingWriter(bitsPerValue, out);
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.primitive.PrimitiveColumnWriter#writeInteger(int)
   */
  @Override
  public void writeInteger(int v) {
    try {
      bitPackingWriter.write(v);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.primitive.PrimitiveColumnWriter#getBufferedSize()
   */
  @Override
  public long getBufferedSize() {
    return out.size();
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.primitive.PrimitiveColumnWriter#getBytes()
   */
  @Override
  public BytesInput getBytes() {
    try {
      this.bitPackingWriter.finish();
      return BytesInput.from(out);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.primitive.PrimitiveColumnWriter#reset()
   */
  @Override
  public void reset() {
    out.reset();
    init();
  }

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.primitive.PrimitiveColumnWriter#getAllocatedSize()
   */
  @Override
  public long getAllocatedSize() {
    return out.getCapacity();
  }


}
