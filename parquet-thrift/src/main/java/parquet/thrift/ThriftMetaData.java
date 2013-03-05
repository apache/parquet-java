/**
 * Copyright 2012 Twitter, Inc.
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
package parquet.thrift;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TBase;

import parquet.hadoop.BadConfigurationException;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.StructType;


public class ThriftMetaData {

  private static final String THRIFT_CLASS = "thrift.class";
  private static final String THRIFT_DESCRIPTOR = "thrift.descriptor";
  private final Class<?> thriftClass;
  private final StructType descriptor;

  public ThriftMetaData(Class<?> thriftClass, StructType descriptor) {
    this.thriftClass = thriftClass;
    this.descriptor = descriptor;
  }

  public Class<?> getThriftClass() {
    return thriftClass;
  }

  public StructType getDescriptor() {
    return descriptor;
  }

  public static ThriftMetaData fromExtraMetaData(
      Map<String, String> extraMetaData) {
    final String thriftClassName = extraMetaData.get(THRIFT_CLASS);
    final String thriftDescriptorString = extraMetaData.get(THRIFT_DESCRIPTOR);
    if (thriftClassName == null && thriftDescriptorString == null) {
      return null;
    }
    Class<?> thriftClass;
    try {
      thriftClass = Class.forName(thriftClassName);
      if (!TBase.class.isAssignableFrom(thriftClass)) {
        throw new BadConfigurationException("Provided class " + thriftClassName + " does not extend TBase");
      }
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Could not instanciate thrift class " + thriftClassName, e);
    }
    final StructType descriptor;
    try {
      descriptor = (StructType)ThriftType.fromJSON(thriftDescriptorString);
    } catch (RuntimeException e) {
      throw new BadConfigurationException("Could not read the thrift descriptor " + thriftDescriptorString, e);
    }
    return new ThriftMetaData(thriftClass, descriptor);
  }

  public Map<String, String> toExtraMetaData() {
    final Map<String, String> map = new HashMap<String, String>();
    map.put(THRIFT_CLASS, thriftClass.getName());
    map.put(THRIFT_DESCRIPTOR, descriptor.toJSON());
    return map;
  }

}
