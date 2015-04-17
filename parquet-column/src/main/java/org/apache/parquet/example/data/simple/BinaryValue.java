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
package org.apache.parquet.example.data.simple;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;


public class BinaryValue extends Primitive {

  private final Binary binary;

  public BinaryValue(Binary binary) {
    this.binary = binary;
  }

  @Override
  public Binary getBinary() {
    return binary;
  }

  @Override
  public String getString() {
    return binary.toStringUsingUTF8();
  }

  @Override
  public void writeValue(RecordConsumer recordConsumer) {
    recordConsumer.addBinary(binary);
  }

  @Override
  public String toString() {
    return getString();
  }
}
