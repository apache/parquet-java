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
package parquet.io.api;

/**
 * Top-level class which should be implemented in order to materialize objects from
 * a stream of Parquet data.
 * 
 * Each record will be wrapped by {@link GroupConverter#start()} and {@link GroupConverter#end()},
 * between which the appropriate fields will be materialized.
 *
 * @author Julien Le Dem
 *
 * @param <T> the materialized object class
 */
abstract public class RecordMaterializer<T> {

  /**
   * @return the result of the conversion
   */
  abstract public T getCurrentRecord();

  /**
   * Called if {@link #getCurrentRecord()} isn't going to be called.
   */
  public void skipCurrentRecord() { }

  /**
   * @return the root converter for this tree
   */
  abstract public GroupConverter getRootConverter();
}
