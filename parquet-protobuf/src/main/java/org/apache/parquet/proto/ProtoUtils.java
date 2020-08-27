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
package org.apache.parquet.proto;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

public final class ProtoUtils {

  private ProtoUtils() {
  }

  public static MessageOrBuilder loadDefaultInstance(
      Class<? extends Message> message) {
    try {
      return (MessageOrBuilder) (message.getMethod("getDefaultInstance")
          .invoke(null));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Cannot load protobuf message class: " + message.getName(), e);
    }
  }

  @SuppressWarnings("unchecked")
  public static MessageOrBuilder loadDefaultInstance(String clazz) {
    try {
      return loadDefaultInstance(
          (Class<? extends Message>) Class.forName(clazz));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Cannot load protobuf message class: " + clazz, e);
    }
  }

}
