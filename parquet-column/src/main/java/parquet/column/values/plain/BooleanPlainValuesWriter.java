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

import static parquet.column.Encoding.PLAIN;
import static parquet.column.values.bitpacking.Packer.LITTLE_ENDIAN;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.bitpacking.ByteBitPackingValuesWriter;


/**
 * An implementation of the PLAIN encoding
 *
 * @author Julien Le Dem
 *
 */
public class BooleanPlainValuesWriter extends ValuesWriter {

  private ByteBitPackingValuesWriter bitPackingWriter;

  public BooleanPlainValuesWriter() {
    bitPackingWriter = new ByteBitPackingValuesWriter(1, LITTLE_ENDIAN);
  }

  @Override
  public final void writeBoolean(boolean v) {
    bitPackingWriter.writeInteger(v ? 1 : 0);
  }

  @Override
  public long getBufferedSize() {
    return bitPackingWriter.getBufferedSize();
  }

  @Override
  public BytesInput getBytes() {
    return bitPackingWriter.getBytes();
  }

  @Override
  public void reset() {
    bitPackingWriter.reset();
  }

  @Override
  public long getAllocatedSize() {
    return bitPackingWriter.getAllocatedSize();
  }

  @Override
  public Encoding getEncoding() {
    return PLAIN;
  }

  @Override
  public String memUsageString(String prefix) {
    return bitPackingWriter.memUsageString(prefix);
  }

}
