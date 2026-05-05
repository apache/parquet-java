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
package org.apache.parquet.io.api;

import org.apache.parquet.io.ParquetDecodingException;

/**
 * Top-level class which should be implemented in order to materialize objects from
 * a stream of Parquet data.
 * <p>
 * Each record will be wrapped by {@link GroupConverter#start()} and {@link GroupConverter#end()},
 * between which the appropriate fields will be materialized.
 *
 * @param <T> the materialized object class
 */
public abstract class RecordMaterializer<T> {

  /**
   * @return the result of the conversion
   * @throws RecordMaterializationException to signal that a record cannot be materialized, but can be skipped
   */
  public abstract T getCurrentRecord();

  /**
   * Called if {@link #getCurrentRecord()} isn't going to be called.
   */
  public void skipCurrentRecord() {}

  /**
   * @return the root converter for this tree
   */
  public abstract GroupConverter getRootConverter();

  /**
   * This exception signals that the current record is cannot be converted from parquet columns to a materialized
   * record, but can be skipped if requested. This exception should be used to signal errors like a union with no
   * set values, or an error in converting parquet primitive values to a materialized record. It should not
   * be used to signal unrecoverable errors, like a data column being corrupt or unreadable.
   */
  public static class RecordMaterializationException extends ParquetDecodingException {
    public RecordMaterializationException() {
      super();
    }

    public RecordMaterializationException(String message, Throwable cause) {
      super(message, cause);
    }

    public RecordMaterializationException(String message) {
      super(message);
    }

    public RecordMaterializationException(Throwable cause) {
      super(cause);
    }
  }
}
