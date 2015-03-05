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
package parquet.bytes;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class ConcatenatingByteArrayCollector extends BytesInput {
  private final List<byte[]> slabs = new ArrayList<byte[]>();
  private long size = 0;

  public void collect(BytesInput bytesInput) throws IOException {
    byte[] bytes = bytesInput.toByteArray();
    slabs.add(bytes);
    size += bytes.length;
  }

  public void reset() {
    size = 0;
    slabs.clear();
  }

  @Override
  public void writeAllTo(OutputStream out) throws IOException {
    for (byte[] slab : slabs) {
      out.write(slab);
    }
  }

  @Override
  public long size() {
    return size;
  }

  /**
   * @param prefix  a prefix to be used for every new line in the string
   * @return a text representation of the memory usage of this structure
   */
  public String memUsageString(String prefix) {
    return format("%s %s %d slabs, %,d bytes", prefix, getClass().getSimpleName(), slabs.size(), size);
  }

}
