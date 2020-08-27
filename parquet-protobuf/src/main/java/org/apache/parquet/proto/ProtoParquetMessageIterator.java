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

import java.io.IOException;
import java.util.Iterator;

import org.apache.parquet.hadoop.ParquetReader;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;

public class ProtoParquetMessageIterator<T extends Message> implements Iterator<T> {

  private final ParquetReader<MessageOrBuilder> reader;
  private boolean hasNext;
  private T currentMessage;

  private ProtoParquetMessageIterator(ParquetReader<MessageOrBuilder> reader) {
    this.reader = reader;
    this.hasNext = true;
    this.currentMessage = null;
  }

  public static <T extends Message> ProtoParquetMessageIterator<T> wrap(
      ParquetReader<MessageOrBuilder> reader) {
    return new ProtoParquetMessageIterator<>(reader);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean hasNext() {
    if (this.currentMessage != null) {
      return true;
    }
    try {
      MessageOrBuilder message = this.reader.read();
      if (message == null) {
        hasNext = false;
      } else {
        if (message instanceof Builder) {
          this.currentMessage = (T) ((Builder) message).build();
        } else if (message instanceof Message) {
          this.currentMessage = (T) message;
        }
        this.hasNext = true;
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to read next protobuf", e);
    }
    return hasNext;
  }

  @Override
  public T next() {
    T rtnValue = this.currentMessage;
    this.currentMessage = null;
    return rtnValue;
  }
}
