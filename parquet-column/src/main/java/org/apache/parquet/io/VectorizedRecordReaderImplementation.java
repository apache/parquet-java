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
package org.apache.parquet.io;

import org.apache.parquet.io.vector.ObjectColumnVector;
import org.apache.parquet.schema.MessageType;

class VectorizedRecordReaderImplementation<T> extends VectorizedRecordReader<T> {
  private RecordReaderImplementation<T> recordReaderImplementation;

  public VectorizedRecordReaderImplementation(RecordReaderImplementation recordReaderImplementation) {
    this.recordReaderImplementation = recordReaderImplementation;
  }

  @Override
  public T read() {
    return recordReaderImplementation.read();
  }

  public boolean shouldSkipCurrentRecord() {
    return recordReaderImplementation.shouldSkipCurrentRecord();
  }

  @Override
  public void readVectors(ColumnVector[] vectors, MessageType[] columnSchemas, long current, long total) {
    recordReaderImplementation.readVectors(vectors, columnSchemas, current, total);
  }

  @Override
  public void readVector(ObjectColumnVector<T> vector, long current, long total) {
    recordReaderImplementation.readVector(vector, current, total);
  }

  //VisibleForTesting
  RecordReaderImplementation<T> getRecordReaderImplementation()
  {
    return recordReaderImplementation;
  }
}
