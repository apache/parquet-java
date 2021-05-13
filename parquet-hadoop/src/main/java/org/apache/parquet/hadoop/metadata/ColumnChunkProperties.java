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
package org.apache.parquet.hadoop.metadata;

import java.util.Arrays;
import java.util.Set;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

public class ColumnChunkProperties {

  private static Canonicalizer<ColumnChunkProperties> properties = new Canonicalizer<>();

  /**
   * @param path the path of this column in the write schema
   * @param type the primitive type of this column
   * @param codec the compression codec used for this column
   * @param encodings a set of encodings used by this column
   * @return column chunk properties
   * @deprecated will be removed in 2.0.0. Use {@link #get(ColumnPath, PrimitiveType, CompressionCodecName, Set)}
   *             instead.
   */
  @Deprecated
  public static ColumnChunkProperties get(ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec, Set<Encoding> encodings) {
    return get(path, new PrimitiveType(Type.Repetition.OPTIONAL, type, ""), codec, encodings);
  }

  public static ColumnChunkProperties get(ColumnPath path, PrimitiveType type, CompressionCodecName codec,
      Set<Encoding> encodings) {
    return properties.canonicalize(new ColumnChunkProperties(codec, path, type, encodings));
  }

  private final CompressionCodecName codec;
  private final ColumnPath path;
  private final PrimitiveType type;
  private final Set<Encoding> encodings;

  private ColumnChunkProperties(CompressionCodecName codec,
                                ColumnPath path,
                                PrimitiveType type,
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

  /**
   * @return the primitive type name for the column
   * @deprecated will be removed in 2.0.0. Use {@link #getPrimitiveType()} instead.
   */
  @Deprecated
  public PrimitiveTypeName getType() {
    return type.getPrimitiveTypeName();
  }

  /**
   * @return the primitive type object for the column
   */
  public PrimitiveType getPrimitiveType() {
    return type;
  }

  public Set<Encoding> getEncodings() {
    return encodings;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ColumnChunkProperties) {
      ColumnChunkProperties other = (ColumnChunkProperties)obj;
      return other.codec == codec && other.path.equals(path) && other.type.equals(type) && equals(other.encodings, encodings);
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
