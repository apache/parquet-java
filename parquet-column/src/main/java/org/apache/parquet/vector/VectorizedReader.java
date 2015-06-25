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
package org.apache.parquet.vector;

import java.io.Closeable;
import java.io.IOException;

public interface VectorizedReader<T> extends Closeable {

  /**
   * check if there is more batch of rows to read.
   * @return true if there is batch to read, and false if there is no batch.
   */
  //TODO should we use this to check EOF?
  //boolean hasNext();

  /**
   * Reads the next batch of rows. This method is used for reading primitive types
   * and does not call the converters at all.
   * @param previous a row batch object to be reused by the reader if possible
   * @return the row batch that was read
   * @throws java.io.IOException
   */
  RowBatch nextBatch(RowBatch previous) throws IOException;

  /**
   * Reads the next batch of rows. This method is used for reading complex types
   * or arbitrary objects and calls the converters eventually to materialize the record.
   * @param previous a row batch object to be reused by the reader if possible
   * @param clazz the class of the record type that will be filled into the column vector
   * @return the row batch that was read
   * @throws java.io.IOException
   */
  RowBatch nextBatch(RowBatch previous, Class<T> clazz) throws IOException;
}
