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
package parquet.column;

/**
 * Container which can construct writers for multiple columns to be stored
 * together.
 *
 * @author Julien Le Dem
 */
public interface ColumnWriteStore {
  /**
   * @param path the column for which to create a writer
   * @return the column writer for the given column
   */
  abstract public ColumnWriter getColumnWriter(ColumnDescriptor path);

  /**
   * when we are done writing to flush to the underlying storage
   */
  abstract public void flush();

  /**
   * called to notify of record boundaries
   */
  abstract public void endRecord();

  /**
   * used for information
   * @return approximate size used in memory
   */
  abstract public long getAllocatedSize();

  /**
   * used to flush row groups to disk
   * @return approximate size of the buffered encoded binary data
   */
  abstract public long getBufferedSize();

  /**
   * used for debugging pupose
   * @return a formated string representing memory usage per column
   */
  abstract public String memUsageString();
}
