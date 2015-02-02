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
package parquet.hadoop.codec;

/**
 * This exception will be thrown when the codec is not supported by parquet, meaning there is no
 * matching codec defined in {@link parquet.hadoop.metadata.CompressionCodecName}
 */
public class CompressionCodecNotSupportedException extends RuntimeException {
  private final Class codecClass;

  public CompressionCodecNotSupportedException(Class codecClass) {
    super("codec not supported: " + codecClass.getName());
    this.codecClass = codecClass;
  }

  public Class getCodecClass() {
    return codecClass;
  }
}
