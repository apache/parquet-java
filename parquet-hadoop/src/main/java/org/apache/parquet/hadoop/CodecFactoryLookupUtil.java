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
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.compression.CompressionCodecFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;

public class CodecFactoryLookupUtil {
  public static String CODEC_PROVIDER_NAME = "parquet.codec.provider.name";
  public static String CODEC_PROVIDER_VERSION = "parquet.codec.provider.version";
  public static String CODEC_PROVIDER_CLASSNAME = "parquet.codec.provider.classname";

  public static CompressionCodecFactory lookup(Map<String,String> metaData, Configuration conf) {
    String codecFactoryClassName = metaData.get(CODEC_PROVIDER_CLASSNAME);
    if (codecFactoryClassName == null) {
      return null;
    }
    try {
      Class<?> codecFactoryClass;
      try {
        codecFactoryClass = Class.forName(codecFactoryClassName);
      } catch (ClassNotFoundException e) {
        // Try to load the class using the job classloader
        codecFactoryClass = conf.getClassLoader().loadClass(codecFactoryClassName);
      }
      CompressionCodecFactory codecFactory = (CompressionCodecFactory) ReflectionUtils.newInstance(codecFactoryClass, conf);
      return codecFactory;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecFactoryClassName + " was not found", e);
    }
  }

  public static CompressionCodecFactory lookup(List<CompressionCodecFactory> codecFactoryList, CompressionCodecName codecName){
    for (CompressionCodecFactory codecFactory : codecFactoryList){
      if (codecFactory.support(codecName)) {
        return codecFactory;
      }
    }
    return null;
  }

  public static <T> CompressionCodecFactory lookup(WriteSupport<T> writeSupport, Configuration conf) {
    if (writeSupport != null) {
      WriteContext writeCtx = writeSupport.init(conf);
      Map<String,String> kvMetaData = writeCtx.getExtraMetaData();
      return lookup(kvMetaData,conf);
    } else {
      return null;
    }
  }

  public static <T> CompressionCodecFactory lookup(WriteSupport<T> writeSupport, int pageSize, Configuration conf,CompressionCodecName codecName) {
    CompressionCodecFactory codecFactoryFromWriteSupport = CodecFactoryLookupUtil.lookup(writeSupport,conf);
    CompressionCodecFactory codecFactoryInOption = new CodecFactory(conf, pageSize);
    List<CompressionCodecFactory> codecFactoryList = new ArrayList<CompressionCodecFactory>();
    if (codecFactoryFromWriteSupport != null) {
      codecFactoryList.add(codecFactoryFromWriteSupport);
    }
    if (codecFactoryInOption != null) {
      codecFactoryList.add(codecFactoryInOption);
    }
    CompressionCodecFactory codecFactory = CodecFactoryLookupUtil.lookup(codecFactoryList,codecName);
    return codecFactory;
  }
}
