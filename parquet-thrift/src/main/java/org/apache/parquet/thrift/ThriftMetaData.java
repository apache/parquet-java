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
package org.apache.parquet.thrift;
import java.util.*;

import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata for thrift stored in the file footer
 */
public class ThriftMetaData {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftMetaData.class);

  private static final String THRIFT_CLASS = "thrift.class";
  private static final String THRIFT_DESCRIPTOR = "thrift.descriptor";
  private Class<?> thriftClass;
  private final String thriftClassName;
  private final StructType descriptor;

  /**
   * @param thriftClassName the class used to serialize
   * @param descriptor the json representation of the thrift structure
   */
  public ThriftMetaData(String thriftClassName, StructType descriptor) {
    this.thriftClassName = thriftClassName;
    this.descriptor = descriptor;
  }

  /**
   * Get the Thrift Class encoded in the metadata.
   * @return Thrift Class encoded in the metadata.
   * @throws BadConfigurationException if the encoded class does not
   * extend TBase or is not available in the current classloader.
   */
  public Class<?> getThriftClass() {
    if (thriftClass == null) {
      thriftClass = getThriftClass(thriftClassName);
    }
    return thriftClass;
  }

  /**
   * @param thriftClassName the name of the thrift class
   * @return the class
   */
  public static Class<?> getThriftClass(String thriftClassName) {
    try {
      Class<?> thriftClass = Class.forName(thriftClassName);
      return thriftClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Could not instantiate thrift class " + thriftClassName, e);
    }
  }

  /**
   * @return the thrift descriptor
   */
  public StructType getDescriptor() {
    return descriptor;
  }

  /**
   * Reads ThriftMetadata from the parquet file footer.
   *
   * @param extraMetaData  extraMetaData field of the parquet footer
   * @return the ThriftMetaData used to write a data file
   */
  public static ThriftMetaData fromExtraMetaData(
      Map<String, String> extraMetaData) {
    final String thriftClassName = extraMetaData.get(THRIFT_CLASS);
    final String thriftDescriptorString = extraMetaData.get(THRIFT_DESCRIPTOR);
    if (thriftClassName == null || thriftDescriptorString == null) {
      return null;
    }
    final StructType descriptor = parseDescriptor(thriftDescriptorString);
    return new ThriftMetaData(thriftClassName, descriptor);
  }

  /**
   * Creates ThriftMetaData from a Thrift-generated class.
   *
   * @param thriftClass a Thrift-generated class
   * @return ThriftMetaData for the given class
   */
  @SuppressWarnings("unchecked")
  public static ThriftMetaData fromThriftClass(Class<?> thriftClass) {
    if (thriftClass != null && TBase.class.isAssignableFrom(thriftClass)) {
      Class<? extends TBase<?, ?>> tClass = (Class<? extends TBase<?, ?>>) thriftClass;
      StructType descriptor = new ThriftSchemaConverter().toStructType(tClass);
      return new ThriftMetaData(thriftClass.getName(), descriptor);
    }
    return null;
  }

  private static StructType parseDescriptor(String json) {
    try {
      return (StructType)ThriftType.fromJSON(json);
    } catch (RuntimeException e) {
      throw new BadConfigurationException("Could not read the thrift descriptor " + json, e);
    }
  }

  /**
   * generates a map of key values to store in the footer
   * @return the key values
   */
  public Map<String, String> toExtraMetaData() {
    final Map<String, String> map = new HashMap<String, String>();
    map.put(THRIFT_CLASS, getThriftClass().getName());
    map.put(THRIFT_DESCRIPTOR, descriptor.toJSON());
    return map;
  }

  /**
   * @param fileMetadata the merged metadata from ultiple files
   * @return the list of thrift classes used to write them
   */
  public static Set<String> getThriftClassNames(Map<String, Set<String>> fileMetadata) {
    return fileMetadata.get(THRIFT_CLASS);
  }

  @Override
  public String toString() {
    return String.format("ThriftMetaData(thriftClassName: %s, descriptor: %s)", thriftClassName, descriptor);
  }
}
