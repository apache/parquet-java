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
package parquet.io;

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;

/**
 * used to read empty schema
 *
 * @author Mickael Lacour <m.lacour@criteo.com>
 *
 * @param <T> the type of the materialized record
 */
class EmptyRecordReader<T> extends RecordReader<T> {

  private final GroupConverter recordConsumer;
  private final RecordMaterializer<T> recordMaterializer;

  public EmptyRecordReader(RecordMaterializer<T> recordMaterializer) {
    this.recordMaterializer = recordMaterializer;
    this.recordConsumer = recordMaterializer.getRootConverter(); // TODO: validator(wrap(recordMaterializer), validating, root.getType());
  }

  /**
   * @see parquet.io.RecordReader#read()
   */
  @Override
  public T read() {
    recordConsumer.start();
    recordConsumer.end();
    return recordMaterializer.getCurrentRecord();
  }
}
