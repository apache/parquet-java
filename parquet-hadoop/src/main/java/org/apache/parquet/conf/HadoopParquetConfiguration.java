/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.conf;

import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of the Parquet configuration interface relying on Hadoop's
 * Configuration to aid with interoperability and backwards compatibility.
 */
public class HadoopParquetConfiguration implements ParquetConfiguration {

  private final Configuration configuration;

  public HadoopParquetConfiguration() {
    this(true);
  }

  public HadoopParquetConfiguration(boolean loadDefaults) {
    configuration = new Configuration(loadDefaults);
  }

  public HadoopParquetConfiguration(Configuration conf) {
    configuration = conf;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public void set(String name, String value) {
    configuration.set(name, value);
  }

  @Override
  public void setLong(String name, long value) {
    configuration.setLong(name, value);
  }

  @Override
  public void setInt(String name, int value) {
    configuration.setInt(name, value);
  }

  @Override
  public void setBoolean(String name, boolean value) {
    configuration.setBoolean(name, value);
  }

  @Override
  public void setStrings(String name, String... values) {
    configuration.setStrings(name, values);
  }

  @Override
  public void setClass(String name, Class<?> value, Class<?> xface) {
    configuration.setClass(name, value, xface);
  }

  @Override
  public String get(String name) {
    return configuration.get(name);
  }

  @Override
  public String get(String name, String defaultValue) {
    return configuration.get(name, defaultValue);
  }

  @Override
  public long getLong(String name, long defaultValue) {
    return configuration.getLong(name, defaultValue);
  }

  @Override
  public int getInt(String name, int defaultValue) {
    return configuration.getInt(name, defaultValue);
  }

  @Override
  public boolean getBoolean(String name, boolean defaultValue) {
    return configuration.getBoolean(name, defaultValue);
  }

  @Override
  public String getTrimmed(String name) {
    return configuration.getTrimmed(name);
  }

  @Override
  public String getTrimmed(String name, String defaultValue) {
    return configuration.getTrimmed(name, defaultValue);
  }

  @Override
  public String[] getStrings(String name, String[] defaultValue) {
    return configuration.getStrings(name, defaultValue);
  }

  @Override
  public Class<?> getClass(String name, Class<?> defaultValue) {
    return configuration.getClass(name, defaultValue);
  }

  @Override
  public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface) {
    return configuration.getClass(name, defaultValue, xface);
  }

  @Override
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    return configuration.getClassByName(name);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return configuration.iterator();
  }
}
