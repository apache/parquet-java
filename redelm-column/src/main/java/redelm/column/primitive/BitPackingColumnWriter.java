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
package redelm.column.primitive;

import static redelm.bytes.BytesUtils.getWidthFromMaxInt;
import static redelm.column.primitive.BitPacking.getBitPackingWriter;

import java.io.IOException;

import redelm.bytes.BytesInput;
import redelm.bytes.CapacityByteArrayOutputStream;
import redelm.column.primitive.BitPacking.BitPackingWriter;

public class BitPackingColumnWriter extends PrimitiveColumnWriter {

  private CapacityByteArrayOutputStream out;
  private BitPackingWriter bitPackingWriter;
  private int bitsPerValue;

  public BitPackingColumnWriter(int bound) {
    this.bitsPerValue = getWidthFromMaxInt(bound);
    this.out = new CapacityByteArrayOutputStream(32*1024); // size needed could be small but starting at 32 is really small
    init();
  }

  private void init() {
    this.bitPackingWriter = getBitPackingWriter(bitsPerValue, out);
  }

  public void writeInteger(int v) {
    try {
      bitPackingWriter.write(v);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getMemSize() {
    return out.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      this.bitPackingWriter.finish();
      return BytesInput.from(out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() {
    out.reset();
    init();
  }

  @Override
  public long allocatedSize() {
    return out.getCapacity();
  }


}
