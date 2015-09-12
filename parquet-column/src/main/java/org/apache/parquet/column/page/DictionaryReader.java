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
package org.apache.parquet.column.page;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;

import java.io.IOException;

/**
 * Read a dictionary from dictionary page
 */
public class DictionaryReader {
  private final ColumnDescriptor column;
  private final DictionaryPageReader pageReader;

  public DictionaryReader(ColumnDescriptor column, DictionaryPageReadStore dictionaryPageReadStore) {
    this.column = column;
    this.pageReader = dictionaryPageReadStore.getDictionaryPageReader(column);
  }

  public Dictionary readDictionary() throws IOException {
    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    return dictionaryPage.getEncoding().initDictionary(column, dictionaryPage);
  }

  public int getDictionarySize() {
    return pageReader.getDictionarySize();
  }
}
