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
package org.apache.parquet.hadoop.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;


/**
 * Abstraction to use with {@link org.apache.parquet.hadoop.ParquetOutputFormat} to convert incoming records
 *
 * @param <T> the type of the incoming records
 */
abstract public class WriteSupport<T> {

  /**
   * information to be persisted in the file
   */
  public static final class WriteContext {
    private final MessageType schema;
    private final Map<String, String> extraMetaData;

    /**
     * @param schema the schema of the data
     * @param extraMetaData application specific metadata to add in the file
     */
    public WriteContext(MessageType schema, Map<String, String> extraMetaData) {
      super();
      this.schema = Objects.requireNonNull(schema, "schema cannot be null");
      this.extraMetaData = Collections.unmodifiableMap(Objects.requireNonNull(extraMetaData, "extraMetaData"));
    }
    /**
     * @return the schema of the file
     */
    public MessageType getSchema() {
      return schema;
    }
    /**
     * @return application specific metadata
     */
    public Map<String, String> getExtraMetaData() {
      return extraMetaData;
    }

  }

  /**
   * Information to be added in the file once all the records have been written
   */
  public static final class FinalizedWriteContext {
    private final Map<String, String> extraMetaData;
    // this class exists to facilitate evolution of the API
    // we can add more fields later

    /**
     * @param extraMetaData application specific metadata to add in the file
     */
    public FinalizedWriteContext(Map<String, String> extraMetaData) {
      super();
      this.extraMetaData = Collections.unmodifiableMap(Objects.requireNonNull(extraMetaData, "extraMetaData"));
    }

    /**
     * @return application specific metadata
     */
    public Map<String, String> getExtraMetaData() {
      return extraMetaData;
    }

  }

  /**
   * called first in the task
   * @param configuration the job's configuration
   * @return the information needed to write the file
   */
  public abstract WriteContext init(Configuration configuration);

  /**
   * This will be called once per row group
   * @param recordConsumer the recordConsumer to write to
   */
  public abstract void prepareForWrite(RecordConsumer recordConsumer);

  /**
   * called once per record
   * @param record one record to write to the previously provided record consumer
   */
  public abstract void write(T record);

  /**
   * Called to get a name to identify the WriteSupport object model.
   * If not null, this is added to the file footer metadata.
   * <p>
   * Defining this method will be required in a future API version.
   *
   * @return a String name for file metadata.
   */
  public String getName() {
    return null;
  }

  /**
   * called once in the end after the last record was written
   * @return information to be added in the file
   */
  public FinalizedWriteContext finalizeWrite() {
    return new FinalizedWriteContext(new HashMap<String, String>());
  }

}
