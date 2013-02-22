/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.io.RecordConsumer;
import parquet.schema.MessageType;


/**
 * Abstraction to use with {@link ParquetOutputFormat} to convert incoming records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the incoming records
 */
abstract public class WriteSupport<T> {

  /**
   * information to be persisted in the file
   *
   * @author Julien Le Dem
   *
   */
  public static class WriteContext {
    private final MessageType schema;
    private final Map<String, String> extraMetaData;

    public WriteContext(MessageType schema, Map<String, String> extraMetaData) {
      super();
      this.schema = schema;
      this.extraMetaData = Collections.unmodifiableMap(extraMetaData);
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

}
