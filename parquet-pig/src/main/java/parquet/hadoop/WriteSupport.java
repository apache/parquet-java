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

import java.util.Map;

import parquet.io.RecordConsumer;
import parquet.schema.MessageType;


/**
 * Abstraction to use with {@link RedelmOutputFormat} to convert incoming records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the incoming records
 */
abstract public class WriteSupport<T> {

  /**
   * @param recordConsumer the recordConsumer to write to
   * @param schema the schema of the incoming records
   * @param extraMetaData extra meta data being written to the footer of the file
   */
  public abstract void initForWrite(RecordConsumer recordConsumer, MessageType schema, Map<String, String> extraMetaData);

  /**
   *
   * @param record one record to write
   */
  public abstract void write(T record);

}
