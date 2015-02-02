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
package parquet.thrift;

import org.apache.thrift.protocol.TField;

/**
 * Implements this class to handle when fields get ignored in {@link BufferedProtocolReadToWrite}
 * @author Tianshuo Deng
 */
public abstract class FieldIgnoredHandler {

  /**
   * handle when a record that contains fields that are ignored, meaning that the schema provided does not cover all the columns in data,
   * the record will still be written but with fields that are not defined in the schema ignored.
   * For each record, this method will be called at most once.
   */
  public void handleRecordHasFieldIgnored() {
  }

  /**
   * handle when a field gets ignored,
   * notice the difference between this method and {@link #handleRecordHasFieldIgnored()} is that:
   * for one record, this method maybe called many times when there are multiple fields not defined in the schema.
   *
   * @param field
   */
  public void handleFieldIgnored(TField field) {
  }
}