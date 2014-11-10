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

import static parquet.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.hadoop.MemoryManager;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;


/**
 * Abstraction to use with {@link parquet.hadoop.ParquetOutputFormat} to convert incoming records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the incoming records
 */
abstract public class WriteSupport<T> {

  /**
   * This memory manager is for all the writers in one task.
   * It will be passed down to writers in WriteContext.
   * Subclass could configure the ratio to specify the total managed memory size in init() method.
   */
  private static MemoryManager memoryManager;

  private static synchronized MemoryManager createMemoryManager() {
    if (memoryManager == null) {
      memoryManager = new MemoryManager();
    }
    return memoryManager;
  }

  /**
   * information to be persisted in the file
   *
   * @author Julien Le Dem
   *
   */
  public static final class WriteContext {
    private final MessageType schema;
    private final Map<String, String> extraMetaData;
    private final MemoryManager memoryManager;

    /**
     * @param schema the schema of the data
     * @param extraMetaData application specific metadata to add in the file
     */
    public WriteContext(MessageType schema, Map<String, String> extraMetaData) {
      super();
      this.schema = checkNotNull(schema, "schema");
      this.extraMetaData = Collections.unmodifiableMap(checkNotNull(extraMetaData, "extraMetaData"));
      memoryManager = createMemoryManager();
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

    /**
     * @return memory manager of parquet writers in one task
     */
    public MemoryManager getMemoryManager() {
      return memoryManager;
    }

  }

  /**
   * Information to be added in the file once all the records have been written
   *
   * @author Julien Le Dem
   *
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
      this.extraMetaData = Collections.unmodifiableMap(checkNotNull(extraMetaData, "extraMetaData"));
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
   * called once in the end after the last record was written
   * @return information to be added in the file
   */
  public FinalizedWriteContext finalizeWrite() {
    return new FinalizedWriteContext(new HashMap<String, String>());
  }

}
