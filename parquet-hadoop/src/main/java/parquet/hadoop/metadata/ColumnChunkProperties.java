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
package parquet.hadoop.metadata;

import java.util.Arrays;
import java.util.Set;

import parquet.column.Encoding;
import parquet.common.internal.Canonicalizer;
import parquet.common.schema.ColumnPath;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ColumnChunkProperties {

  private static Canonicalizer<ColumnChunkProperties> properties = new Canonicalizer<ColumnChunkProperties>();

  public static ColumnChunkProperties get(ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec, Set<Encoding> encodings) {
    return properties.canonicalize(new ColumnChunkProperties(codec, path, type, encodings));
  }

  private final CompressionCodecName codec;
  private final ColumnPath path;
  private final PrimitiveTypeName type;
  private final Set<Encoding> encodings;

  private ColumnChunkProperties(CompressionCodecName codec,
                                ColumnPath path,
                                PrimitiveTypeName type,
                                Set<Encoding> encodings) {
    super();
    this.codec = codec;
    this.path = path;
    this.type = type;
    this.encodings = encodings;
  }

  public CompressionCodecName getCodec() {
    return codec;
  }

  public ColumnPath getPath() {
    return path;
  }

  public PrimitiveTypeName getType() {
    return type;
  }

  public Set<Encoding> getEncodings() {
    return encodings;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ColumnChunkProperties) {
      ColumnChunkProperties other = (ColumnChunkProperties)obj;
      return other.codec == codec && other.path.equals(path) && other.type == type && equals(other.encodings, encodings);
    }
    return false;
  }

  private boolean equals(Set<Encoding> a, Set<Encoding> b) {
    return a.size() == b.size() && a.containsAll(b);
  }

  @Override
  public int hashCode() {
    return codec.hashCode() ^ path.hashCode() ^ type.hashCode() ^ Arrays.hashCode(encodings.toArray());
  }

  @Override
  public String toString() {
    return codec + " " + path + " " + type + "  " + encodings;
  }
}
