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
package parquet.example.data.simple;

import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;

public abstract class Primitive {

  public String getString() {
    throw new UnsupportedOperationException();
  }

  public int getInteger() {
    throw new UnsupportedOperationException();
  }

  public long getLong() {
    throw new UnsupportedOperationException();
  }

  public boolean getBoolean() {
    throw new UnsupportedOperationException();
  }

  public Binary getBinary() {
    throw new UnsupportedOperationException();
  }

  public Binary getInt96() {
    throw new UnsupportedOperationException();
  }

  public float getFloat() {
    throw new UnsupportedOperationException();
  }

  public double getDouble() {
    throw new UnsupportedOperationException();
  }

  abstract public void writeValue(RecordConsumer recordConsumer);

}
