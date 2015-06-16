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
package org.apache.parquet.tools.read;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class SimpleReadSupport extends ReadSupport<SimpleRecord> {
  @Override
  public RecordMaterializer<SimpleRecord> prepareForRead(Configuration conf, Map<String,String> metaData, MessageType schema, ReadContext context) {
    return new SimpleRecordMaterializer(schema);
  }

  @Override
  public ReadContext init(InitContext context) {
    return new ReadContext(context.getFileSchema());
  }
}

