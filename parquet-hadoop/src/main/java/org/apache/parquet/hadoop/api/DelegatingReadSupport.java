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
package org.apache.parquet.hadoop.api;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * Helps composing read supports
 *
 * @param <T> the Java class of objects created by this ReadSupport
 */
public class DelegatingReadSupport<T> extends ReadSupport<T> {

  private final ReadSupport<T> delegate;

  public DelegatingReadSupport(ReadSupport<T> delegate) {
    super();
    this.delegate = delegate;
  }

  @Override
  public ReadSupport.ReadContext init(InitContext context) {
    return delegate.init(context);
  }

  @Override
  public RecordMaterializer<T> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadSupport.ReadContext readContext) {
    return delegate.prepareForRead(configuration, keyValueMetaData, fileSchema, readContext);
  }

  @Override
  public String toString() {
    return this.getClass().getName() + "(" + delegate.toString() + ")";
  }
}
