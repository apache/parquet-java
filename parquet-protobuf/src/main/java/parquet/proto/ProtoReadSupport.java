/**
 * Copyright 2013 Lukas Nalezenec
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.proto;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import java.util.Map;


/**
 * @author Lukas Nalezenec
 */
public class ProtoReadSupport<T extends Message> extends ReadSupport<T> {

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
      return new ReadContext(requestedProjection);
    } else {
      return new ReadContext(context.getFileSchema());
    }
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
    String strProtoClass = keyValueMetaData.get(PB_CLASS);

    if (strProtoClass == null) {
      throw new RuntimeException("I Need parameter " + PB_CLASS + " with protobufer class");
    }

    return new ProtoRecordMaterializer(readContext.getRequestedSchema(), Protobufs.getProtobufClass(strProtoClass));
  }


}
