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

import java.util.Map;

/**
 * Configuration interface with the methods necessary to configure for Parquet applications.
 */
public interface ParquetConfiguration extends Iterable<Map.Entry<String,String>> {

  void set(String name, String value);

  void setLong(String name, long value);

  void setInt(String name, int value);

  void setBoolean(String name, boolean value);

  void setStrings(String name, String... value);

  void setClass(String name, Class<?> value, Class<?> xface);

  String get(String name);

  String get(String name, String defaultValue);

  long getLong(String name, long defaultValue);

  int getInt(String name, int defaultValue);

  boolean getBoolean(String name, boolean defaultValue);

  String getTrimmed(String name);

  String getTrimmed(String name, String defaultValue);

  String[] getStrings(String name, String[] defaultValue);

  Class<?> getClass(String name, Class<?> defaultValue);

  <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface);

  Class<?> getClassByName(String name) throws ClassNotFoundException;
}
