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
package org.apache.parquet.benchmarks;

import java.io.IOException;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;

/**
 * Shared helpers for encode/decode micro-benchmarks.
 */
final class BenchmarkEncodingUtils {

  private BenchmarkEncodingUtils() {}

  /**
   * Container for the two artefacts produced by a dictionary-encoded page:
   * the encoded dictionary indices ({@link #dictData}) and the dictionary
   * page itself ({@link #dictPage}). The dictionary page may be {@code null}
   * if the writer fell back to plain encoding (for example, when the
   * dictionary exceeded its configured maximum size).
   */
  static final class EncodedDictionary {
    final byte[] dictData;
    final DictionaryPage dictPage;

    EncodedDictionary(byte[] dictData, DictionaryPage dictPage) {
      this.dictData = dictData;
      this.dictPage = dictPage;
    }

    boolean fellBackToPlain() {
      return dictPage == null;
    }
  }

  /**
   * Drains a {@link DictionaryValuesWriter} into an {@link EncodedDictionary}.
   *
   * <p>The writer's data bytes (the RLE-encoded indices) and the dictionary
   * page are returned separately so both pieces can be measured or fed to a
   * decoder symmetrically. The dictionary page buffer is copied so it remains
   * valid after the writer's allocator is released.
   *
   * <p>The writer is closed via {@code toDictPageAndClose()}; callers must not
   * call {@link DictionaryValuesWriter#close()} again afterwards.
   */
  static EncodedDictionary drainDictionary(DictionaryValuesWriter writer) throws IOException {
    byte[] dictData = writer.getBytes().toByteArray();
    DictionaryPage rawPage = writer.toDictPageAndClose();
    DictionaryPage dictPage = rawPage == null ? null : rawPage.copy();
    return new EncodedDictionary(dictData, dictPage);
  }
}
