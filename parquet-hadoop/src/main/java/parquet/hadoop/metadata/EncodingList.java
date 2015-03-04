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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import parquet.column.Encoding;
import parquet.common.internal.Canonicalizer;

public class EncodingList implements Iterable<Encoding> {

  private static Canonicalizer<EncodingList> encodingLists = new Canonicalizer<EncodingList>();

  public static EncodingList getEncodingList(List<Encoding> encodings) {
    return encodingLists.canonicalize(new EncodingList(encodings));
  }

  private final List<Encoding> encodings;

  private EncodingList(List<Encoding> encodings) {
    super();
    this.encodings = Collections.unmodifiableList(encodings);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EncodingList) {
      List<parquet.column.Encoding> other = ((EncodingList)obj).encodings;
      final int size = other.size();
      if (size != encodings.size()) {
        return false;
      }
      for (int i = 0; i < size; i++) {
        if (!other.get(i).equals(encodings.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 1;
    for (parquet.column.Encoding element : encodings)
      result = 31 * result + (element == null ? 0 : element.hashCode());
    return result;
  }

  public List<Encoding> toList() {
    return encodings;
  }

  @Override
  public Iterator<Encoding> iterator() {
    return encodings.iterator();
  }

  public int size() {
    return encodings.size();
  }

}
