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
package parquet.hadoop.api;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * Abstraction used by the {@link parquet.hadoop.ParquetInputFormat} to materialize records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized record
 */
abstract public class ReadSupport<T> {

  /**
   * information to read the file
   *
   * @author Julien Le Dem
   *
   */
  public static final class ReadContext {
    private final MessageType requestedSchema;

    public ReadContext(MessageType requestedSchema) {
      super();
      if (requestedSchema == null) {
        throw new NullPointerException("requestedSchema");
      }
      this.requestedSchema = requestedSchema;
    }
    /**
     * @return the schema of the file
     */
    public MessageType getRequestedSchema() {
      return requestedSchema;
    }
  }

  /**
   * called in {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)} in the front end
   *
   * @param configuration the job configuration
   * @param keyValueMetaData the app specific metadata from the file
   * @param fileSchema the schema of the file
   * @param requestedSchema the schema requested by the user
   * @return the readContext that defines how to read the file
   */
  abstract public ReadContext init(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema);

  /**
   * called in {@link org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)} in the back end
   * the returned RecordConsumer will materialize the records and add them to the destination
   * @param configuration the job configuration
   * @param keyValueMetaData the app specific metadata from the file
   * @param fileSchema the schema of the file
   * @param readContext returned by the init method
   * @return the recordConsumer that will receive the events
   */
  abstract public RecordMaterializer<T> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext);

}
