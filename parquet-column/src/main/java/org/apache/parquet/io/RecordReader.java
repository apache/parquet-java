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

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.vector.ColumnVector;
import org.apache.parquet.vector.ObjectColumnVector;

/**
 * used to read reassembled records
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized record
 */
public abstract class RecordReader<T> {

  /**
   * Reads one record and returns it.
   * @return the materialized record
   */
  public abstract T read();

  /**
   * Returns whether the current record should be skipped (dropped)
   * Will be called *after* read()
   */
  public boolean shouldSkipCurrentRecord() {
    return false;
  }

  /**
   * Reads a vector of rows into the given vectors.
   * This method is for reading Parquet primitive values and
   * does not call the converters
   * @param vectors the vectors to fill
   * @param current number of loaded records so far
   * @param total total number of records in the row group
   */
  public void readVectors(ColumnVector[] vectors, MessageType[] columnSchemas, long current, long total) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads a vector of rows into the given vectors
   * This method is used for reading complex types and arbitrary objects
   * and calls the converters and materializer to materialize
   * the actual record
   * @param vector the vector to fill
   * @param current number of loaded records so far
   * @param total total number of records in the row group
   */
  public void readVector(ObjectColumnVector<T> vector, long current, long total) {
    throw new UnsupportedOperationException();
  }
}
