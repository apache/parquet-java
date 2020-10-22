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

import java.util.AbstractMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.MessageOrBuilder;


public class ProtoReadSupport<T extends MessageOrBuilder> extends ReadSupport<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoReadSupport.class);

  public static final String PB_REQUESTED_PROJECTION = "parquet.proto.projection";

  @Deprecated
  public static final String PB_CLASS = "parquet.proto.class";
  public static final String PB_TYPE = "parquet.proto.type";
  public static final String PB_DESCRIPTOR = "parquet.proto.descriptor";

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

  @Override
  public ReadContext init(InitContext context) {
    String requestedProjectionString = context.getConfiguration().get(PB_REQUESTED_PROJECTION);

    if (requestedProjectionString != null && !requestedProjectionString.trim().isEmpty()) {
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
  public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
    final Optional<String> headerProtoClass = Optional.ofNullable(keyValueMetaData.get(PB_CLASS));
    final Optional<String> headerProtoType = Optional.ofNullable(keyValueMetaData.get(PB_TYPE));

    final Optional<String> configuredProtoClass = Optional.ofNullable(configuration.get(PB_CLASS));
    final Optional<String> configuredProtoType = Optional.ofNullable(configuration.get(PB_TYPE));

    final String candidateClass;

    /* 
     * Load the class type, with backwards-compatibility for class-only support (no registry).
     * Configured values override any values found in the meta section of the file.
     */

    if (configuredProtoType.isPresent()) {
      // No schema registry implemented yet
      LOG.debug("Configured proto type: {}", configuredProtoType.get());
      candidateClass = parseProtoType(configuredProtoType.get()).getValue();
    } else if (configuredProtoClass.isPresent()) {
      LOG.debug("Configured proto class: {}", configuredProtoClass.get());
      candidateClass = configuredProtoClass.get();
    } else if (headerProtoType.isPresent()) {
      // No schema registry implemented yet
      LOG.debug("Parquet meta proto type: {}", headerProtoType.get());
      candidateClass = parseProtoType(headerProtoType.get()).getValue();
    } else if (headerProtoClass.isPresent()) {
      LOG.debug("Parquet meta proto class: {}", headerProtoClass.get());
      candidateClass = headerProtoClass.get();
    } else {
      throw new IllegalArgumentException("No proto class specified for read");
    }

    LOG.debug("Reading data with Protocol Buffer class {}", candidateClass);

    try {
      MessageType requestedSchema = readContext.getRequestedSchema();
      return new ProtoRecordMaterializer<T>(configuration, requestedSchema,
          ProtoUtils.loadDefaultInstance(candidateClass), keyValueMetaData);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not protobuf class: " + candidateClass, e);
    }
  }

  /**
   * Split a fully qualified protobuf type into its two parts.
   *
   * <pre>
   * schema-registry/class
   * </pre>
   *
   * <pre>
   * type.googleapis.com / google.profile.Person
   * </pre>
   *
   * <ul>
   * <li>type.googleapis.com/google.profile.Person =
   * ["type.googleapis.com","google.profile.Person"]</li>
   * <li>google.profile.Person = ["","google.profile.Person"]</li>
   * </ul>
   *
   * @param protoType The protobuf fully qualifies type
   * @return Entry containing registry and class information
   */
  private Entry<String, String> parseProtoType(final String protoType) {
    final String[] parts = protoType.split("/");
    return (parts.length == 1) ? new AbstractMap.SimpleEntry<>("", parts[0])
        : new AbstractMap.SimpleEntry<>(parts[0], parts[1]);
  }

}
