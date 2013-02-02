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
package redelm.hadoop;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import redelm.io.RecordMaterializer;

import org.apache.hadoop.mapred.InputFormat;

/**
 * Abstraction used by the {@link RedelmInputFormat} to materialize records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized record
 */
abstract public class ReadSupport<T> implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * called in {@link InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)} when the file footer is read
   * @param metaDataBlocks metadata blocks from the footer
   * @param requestedSchema the schema requested by the user
   */
  abstract public void initForRead(Map<String, String> keyValueMetaData, String requestedSchema);

  /**
   * called by the record reader in the backend.
   * the returned RecordConsumer will materialize the records and add them to the destination
   * @return the recordConsumer that will receive the events
   */
  abstract public RecordMaterializer<T> newRecordConsumer();

}
