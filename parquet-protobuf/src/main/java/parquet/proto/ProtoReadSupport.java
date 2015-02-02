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
package parquet.proto;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import parquet.Log;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import java.util.Map;


/**
 * @author Lukas Nalezenec
 */
public class ProtoReadSupport<T extends Message> extends ReadSupport<T> {

  private static final Log LOG = Log.getLog(ProtoReadSupport.class);

  public static final String PB_REQUESTED_PROJECTION = "parquet.proto.projection";

  public static final String PB_CLASS = "parquet.proto.class";
  public static final String PB_DESCRIPTOR = "parquet.proto.descriptor";

  public static void setRequestedProjection(Configuration configuration, String requestedProjection) {
    configuration.set(PB_REQUESTED_PROJECTION, requestedProjection);
  }

  @Override
  public ReadContext init(InitContext context) {
    String requestedProjectionString = context.getConfiguration().get(PB_REQUESTED_PROJECTION);

    if (requestedProjectionString != null && !requestedProjectionString.trim().isEmpty()) {
      MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), requestedProjectionString);
      LOG.debug("Reading data with projection " + requestedProjection);
      return new ReadContext(requestedProjection);
    } else {
      MessageType fileSchema = context.getFileSchema();
      LOG.debug("Reading data with schema " + fileSchema);
      return new ReadContext(fileSchema);
    }
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
    String strProtoClass = keyValueMetaData.get(PB_CLASS);

    if (strProtoClass == null) {
      throw new RuntimeException("I Need parameter " + PB_CLASS + " with Protocol Buffer class");
    }

    LOG.debug("Reading data with Protocol Buffer class" + strProtoClass);

    MessageType requestedSchema = readContext.getRequestedSchema();
    Class<? extends Message> protobufClass = Protobufs.getProtobufClass(strProtoClass);
    return new ProtoRecordMaterializer(requestedSchema, protobufClass);
  }


}
