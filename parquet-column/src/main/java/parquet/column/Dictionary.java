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

import parquet.io.api.Binary;

/**
 * a dictionary to decode dictionary based encodings
 *
 * @author Julien Le Dem
 *
 */
public abstract class Dictionary {

  private final Encoding encoding;

  public Dictionary(Encoding encoding) {
    this.encoding = encoding;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public abstract int getMaxId();

  public Binary decodeToBinary(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public int decodeToInt(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public long decodeToLong(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public float decodeToFloat(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public double decodeToDouble(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public boolean decodeToBoolean(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }
}
