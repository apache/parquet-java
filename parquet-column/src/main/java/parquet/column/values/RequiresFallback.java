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
package parquet.column.values;

import parquet.column.values.fallback.FallbackValuesWriter;

/**
 *
 * Used to add extra behavior to a ValuesWriter that requires fallback
 * @See {@link FallbackValuesWriter}
 *
 * @author Julien Le Dem
 *
 */
public interface RequiresFallback {

  /**
   * In the case of a dictionary based encoding we will fallback if the dictionary becomes too big
   * @return true to notify the parent that we should fallback to another encoding
   */
  boolean shouldFallBack();

  /**
   * Before writing the first page we will verify if the encoding is worth it.
   * and fall back if a simpler encoding would be better in that case
   * @param rawSize the size if encoded with plain
   * @param encodedSize the size as encoded by the current encoding
   * @return true if we keep this encoding
   */
  boolean isCompressionSatisfying(long rawSize, long encodedSize);

  /**
   * When falling back to a different encoding we must re-encode all the values seen so far
   * @param writer the new encoder to write the current values to
   */
  void fallBackAllValuesTo(ValuesWriter writer);

}
