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

import static parquet.Log.DEBUG;
import static parquet.column.primitive.BitPacking.getBitPackingReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Log;
import parquet.column.primitive.BitPacking.BitPackingReader;


public class BooleanPlainColumnReader extends PrimitiveColumnReader {
  private static final Log LOG = Log.getLog(BooleanPlainColumnReader.class);

  private BitPackingReader in;

  @Override
  public boolean readBoolean() {
    try {
      return in.read() == 0 ? false : true;
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }


  /**
   * {@inheritDoc}
   * @see parquet.column.primitive.PrimitiveColumnReader#initFromPage(byte[], int)
   */
  @Override
  public int initFromPage(long valueCount, byte[] in, int offset) throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = getBitPackingReader(1, new ByteArrayInputStream(in, offset, in.length - offset), valueCount);
    return in.length;
  }

}