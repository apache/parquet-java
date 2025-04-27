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
import com.twitter.elephantbird.util.Protobufs;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoReadSupport<T extends Message> extends ReadSupport<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoReadSupport.class);

  public static final String PB_REQUESTED_PROJECTION = "parquet.proto.projection";

  public static final String PB_CLASS = "parquet.proto.class";
  public static final String PB_DESCRIPTOR = "parquet.proto.descriptor";
  public static final String PB_CODEGEN = "parquet.proto.readCodegen";

  public enum CodegenMode {
    OFF {
      @Override
      public boolean ignoreCodeGenException() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean tryCodeGen(Class<? extends Message> protoClass) {
        return false;
      }
    },

    SUPPORT {
      @Override
      public boolean ignoreCodeGenException() {
        return true;
      }

      @Override
      public boolean tryCodeGen(Class<? extends Message> protoClass) {
        return ByteBuddyCodeGen.isGeneratedMessage(protoClass) && ByteBuddyCodeGen.isByteBuddyAvailable(false);
      }
    },

    REQUIRED {
      @Override
      public boolean ignoreCodeGenException() {
        return false;
      }

      @Override
      public boolean tryCodeGen(Class<? extends Message> protoClass) {
        if (!ByteBuddyCodeGen.isGeneratedMessage(protoClass)) {
          throw new UnsupportedOperationException("protoClass is not a GeneratedMessage: " + protoClass);
        }
        return ByteBuddyCodeGen.isByteBuddyAvailable(true);
      }
    };

    public static final ProtoReadSupport.CodegenMode DEFAULT = CodegenMode.SUPPORT;

    public static ProtoReadSupport.CodegenMode orDefault(ProtoReadSupport.CodegenMode codegenMode) {
      return codegenMode == null ? DEFAULT : codegenMode;
    }

    public abstract boolean ignoreCodeGenException();

    public abstract boolean tryCodeGen(Class<? extends Message> protoClass);
  }

  public static void setRequestedProjection(Configuration configuration, String requestedProjection) {
    configuration.set(PB_REQUESTED_PROJECTION, requestedProjection);
  }

  /**
   * Set name of protobuf class to be used for reading data.
   * If no class is set, value from file header is used.
   * Note that the value in header is present only if the file was written
   * using parquet-protobuf project, it will fail otherwise.
   *
   * @param configuration a configuration
   * @param protobufClass a fully-qualified protobuf class name
   */
  public static void setProtobufClass(Configuration configuration, String protobufClass) {
    configuration.set(PB_CLASS, protobufClass);
  }

  public static void setCodegenMode(Configuration configuration, ProtoReadSupport.CodegenMode codegenMode) {
    configuration.setEnum(PB_CODEGEN, codegenMode);
  }

  @Override
  public ReadContext init(InitContext context) {
    String requestedProjectionString = context.getParquetConfiguration().get(PB_REQUESTED_PROJECTION);

    if (requestedProjectionString != null
        && !requestedProjectionString.trim().isEmpty()) {
      MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), requestedProjectionString);
      LOG.debug("Reading data with projection {}", requestedProjection);
      return new ReadContext(requestedProjection);
    } else {
      MessageType fileSchema = context.getFileSchema();
      LOG.debug("Reading data with schema {}", fileSchema);
      return new ReadContext(fileSchema);
    }
  }

  @Override
  public RecordMaterializer<T> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    return prepareForRead(new HadoopParquetConfiguration(configuration), keyValueMetaData, fileSchema, readContext);
  }

  @Override
  public RecordMaterializer<T> prepareForRead(
      ParquetConfiguration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    String headerProtoClass = keyValueMetaData.get(PB_CLASS);
    String configuredProtoClass = configuration.get(PB_CLASS);

    if (configuredProtoClass != null) {
      LOG.debug("Replacing class " + headerProtoClass + " by " + configuredProtoClass);
      headerProtoClass = configuredProtoClass;
    }

    if (headerProtoClass == null) {
      throw new RuntimeException("I Need parameter " + PB_CLASS + " with Protocol Buffer class");
    }

    LOG.debug("Reading data with Protocol Buffer class {}", headerProtoClass);

    MessageType requestedSchema = readContext.getRequestedSchema();
    Class<? extends Message> protobufClass = Protobufs.getProtobufClass(headerProtoClass);
    ProtoRecordMaterializer protoRecordMaterializer =
        new ProtoRecordMaterializer(configuration, requestedSchema, protobufClass, keyValueMetaData);

    CodegenMode codegenMode =
        ProtoReadSupport.CodegenMode.valueOf(configuration.get(PB_CODEGEN, CodegenMode.DEFAULT.name()));
    return codegenMode.tryCodeGen(protobufClass)
        ? ByteBuddyCodeGen.ReadSupport.tryEnhanceRecordMaterializer(
            protoRecordMaterializer, codegenMode, configuration)
        : protoRecordMaterializer;
  }
}
