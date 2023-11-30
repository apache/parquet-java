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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Configuration for Parquet without Hadoop dependency.
 */
public class PlainParquetConfiguration implements ParquetConfiguration {

  private final Map<String, String> map;

  public PlainParquetConfiguration() {
    map = new HashMap<>();
  }

  public PlainParquetConfiguration(Map<String, String> properties) {
    map = new HashMap<>(properties);
  }

  @Override
  public void set(String s, String s1) {
    map.put(s, s1);
  }

  @Override
  public void setLong(String name, long value) {
    set(name, String.valueOf(value));
  }

  @Override
  public void setInt(String name, int value) {
    set(name, String.valueOf(value));
  }

  @Override
  public void setBoolean(String name, boolean value) {
    set(name, String.valueOf(value));
  }

  @Override
  public void setStrings(String name, String... value) {
    if (value.length > 0) {
      StringBuilder sb = new StringBuilder(value[0]);
      for (int i = 1; i < value.length; ++i) {
        sb.append(',');
        sb.append(value[i]);
      }
      set(name, sb.toString());
    } else {
      set(name, "");
    }
  }

  @Override
  public void setClass(String name, Class<?> value, Class<?> xface) {
    if (xface.isAssignableFrom(value)) {
      set(name, value.getName());
    } else {
      throw new RuntimeException(
          xface.getCanonicalName() + " is not assignable from " + value.getCanonicalName());
    }
  }

  @Override
  public String get(String name) {
    return map.get(name);
  }

  @Override
  public String get(String name, String defaultValue) {
    String value = get(name);
    if (value != null) {
      return value;
    } else {
      return defaultValue;
    }
  }

  @Override
  public long getLong(String name, long defaultValue) {
    String value = get(name);
    if (value != null) {
      return Long.parseLong(value);
    } else {
      return defaultValue;
    }
  }

  @Override
  public int getInt(String name, int defaultValue) {
    String value = get(name);
    if (value != null) {
      return Integer.parseInt(value);
    } else {
      return defaultValue;
    }
  }

  @Override
  public boolean getBoolean(String name, boolean defaultValue) {
    String value = get(name);
    if (value != null) {
      return Boolean.parseBoolean(value);
    } else {
      return defaultValue;
    }
  }

  @Override
  public String getTrimmed(String name) {
    String value = get(name);
    if (value != null) {
      return value.trim();
    } else {
      return null;
    }
  }

  @Override
  public String getTrimmed(String name, String defaultValue) {
    String value = get(name);
    if (value != null) {
      return value.trim();
    } else {
      return defaultValue;
    }
  }

  @Override
  public String[] getStrings(String name, String[] defaultValue) {
    String value = get(name);
    if (value != null) {
      return value.split(",");
    } else {
      return defaultValue;
    }
  }

  @Override
  public Class<?> getClass(String name, Class<?> defaultValue) {
    String value = get(name);
    if (value != null) {
      try {
        return Class.forName(value);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } else {
      return defaultValue;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface) {
    Class<?> value = getClass(name, defaultValue);
    if (value != null && value.isAssignableFrom(xface)) {
      return (Class<? extends U>) value;
    }
    return defaultValue;
  }

  @Override
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    return Class.forName(name);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return map.entrySet().iterator();
  }
}
