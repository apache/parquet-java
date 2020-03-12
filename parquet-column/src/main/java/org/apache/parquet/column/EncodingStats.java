/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.column;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;

/**
 * EncodingStats track dictionary and data page encodings for a single column within a row group.
 * These are used when filtering row groups. For example, to filter a row group based on a column's
 * dictionary, all of the data pages in that column must be dictionary-encoded. This class provides
 * convenience methods for those checks, like {@link #hasNonDictionaryEncodedPages()}.
 */
public class EncodingStats {
  final Map<Encoding, Number> dictStats;
  final Map<Encoding, Number> dataStats;
  private final boolean usesV2Pages;

  private EncodingStats(Map<Encoding, Number> dictStats,
                        Map<Encoding, Number> dataStats,
                        boolean usesV2Pages) {
    this.dictStats = dictStats;
    this.dataStats = dataStats;
    this.usesV2Pages = usesV2Pages;
  }

  public Set<Encoding> getDictionaryEncodings() {
    return dictStats.keySet();
  }

  public Set<Encoding> getDataEncodings() {
    return dataStats.keySet();
  }

  public int getNumDictionaryPagesEncodedAs(Encoding enc) {
    final Number pageCount = dictStats.get(enc);
    return (pageCount == null) ? 0 : pageCount.intValue();
  }

  public int getNumDataPagesEncodedAs(Encoding enc) {
    final Number pageCount = dataStats.get(enc);
    return (pageCount == null) ? 0 : pageCount.intValue();
  }

  public boolean hasDictionaryPages() {
    return !dictStats.isEmpty();
  }

  public boolean hasDictionaryEncodedPages() {
    Set<Encoding> encodings = dataStats.keySet();
    return (encodings.contains(RLE_DICTIONARY) || encodings.contains(PLAIN_DICTIONARY));
  }

  public boolean hasNonDictionaryEncodedPages() {
    if (dataStats.isEmpty()) {
      return false; // no pages
    }

    // this modifies the set, so copy it
    Set<Encoding> encodings = new HashSet<>(dataStats.keySet());
    if (!encodings.remove(RLE_DICTIONARY) &&
        !encodings.remove(PLAIN_DICTIONARY)) {
      return true; // not dictionary encoded
    }

    if (encodings.isEmpty()) {
      return false;
    }

    // at least one non-dictionary encoding is present
    return true;
  }

  public boolean usesV2Pages() {
    return usesV2Pages;
  }

  /**
   * Used to build {@link EncodingStats} from metadata or to accumulate stats as pages are written.
   */
  public static class Builder {
    private final Map<Encoding, AtomicInteger> dictStats = new LinkedHashMap<>();
    private final Map<Encoding, AtomicInteger> dataStats = new LinkedHashMap<>();
    private boolean usesV2Pages = false;

    public Builder clear() {
      this.usesV2Pages = false;
      dictStats.clear();
      dataStats.clear();
      return this;
    }

    public Builder withV2Pages() {
      this.usesV2Pages = true;
      return this;
    }

    public Builder addDictEncoding(Encoding encoding) {
      return addDictEncoding(encoding, 1);
    }

    public Builder addDictEncoding(Encoding encoding, int numPages) {
      dictStats.computeIfAbsent(encoding, enc -> new AtomicInteger(0))
          .addAndGet(numPages);
      return this;
    }

    public Builder addDataEncodings(Collection<Encoding> encodings) {
      for (Encoding encoding : encodings) {
        addDataEncoding(encoding);
      }
      return this;
    }

    public Builder addDataEncoding(Encoding encoding) {
      return addDataEncoding(encoding, 1);
    }

    public Builder addDataEncoding(Encoding encoding, int numPages) {
      dataStats.computeIfAbsent(encoding, enc -> new AtomicInteger(0))
      .addAndGet(numPages);
      return this;
    }

    public EncodingStats build() {
      return new EncodingStats(
          Collections.unmodifiableMap(new LinkedHashMap<>(dictStats)),
          Collections.unmodifiableMap(new LinkedHashMap<>(dataStats)),
          usesV2Pages);
    }
  }
}
