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

package org.apache.parquet.column;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * Represents a Parquet property that may have different values for the different columns.
 */
abstract class ColumnProperty<T> {
  private static class DefaultColumnProperty<T> extends ColumnProperty<T> {
    private final T defaultValue;

    private DefaultColumnProperty(T defaultValue) {
      this.defaultValue = defaultValue;
    }

    @Override
    public T getDefaultValue() {
      return defaultValue;
    }

    @Override
    public T getValue(ColumnPath columnPath) {
      return getDefaultValue();
    }

    @Override
    public String toString() {
      return Objects.toString(getDefaultValue());
    }
  }

  private static class MultipleColumnProperty<T> extends DefaultColumnProperty<T> {
    private final Map<ColumnPath, T> values;

    private MultipleColumnProperty(T defaultValue, Map<ColumnPath, T> values) {
      super(defaultValue);
      assert !values.isEmpty();
      this.values = new HashMap<>(values);
    }

    @Override
    public T getValue(ColumnPath columnPath) {
      T value = values.get(columnPath);
      if (value != null) {
        return value;
      }
      return getDefaultValue();
    }

    @Override
    public String toString() {
      return Objects.toString(getDefaultValue()) + ' ' + values.toString();
    }
  }

  static class Builder<T> {
    private T defaultValue;
    private final Map<ColumnPath, T> values = new HashMap<>();

    private Builder() {
    }

    public Builder<T> withDefaultValue(T defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder<T> withValue(ColumnPath columnPath, T value) {
      values.put(columnPath, value);
      return this;
    }

    public Builder<T> withValue(String columnPath, T value) {
      return withValue(ColumnPath.fromDotString(columnPath), value);
    }

    public Builder<T> withValue(ColumnDescriptor columnDescriptor, T value) {
      return withValue(ColumnPath.get(columnDescriptor.getPath()), value);
    }

    public ColumnProperty<T> build() {
      if (values.isEmpty()) {
        return new DefaultColumnProperty<>(defaultValue);
      } else {
        return new MultipleColumnProperty<>(defaultValue, values);
      }
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static <T> Builder<T> builder(ColumnProperty<T> toCopy) {
    Builder<T> builder = new Builder<>();
    builder.withDefaultValue(((DefaultColumnProperty<T>) toCopy).defaultValue);
    if (toCopy instanceof MultipleColumnProperty) {
      builder.values.putAll(((MultipleColumnProperty<T>) toCopy).values);
    }
    return builder;
  }

  public abstract T getDefaultValue();

  public abstract T getValue(ColumnPath columnPath);

  public T getValue(String columnPath) {
    return getValue(ColumnPath.fromDotString(columnPath));
  }

  public T getValue(ColumnDescriptor columnDescriptor) {
    return getValue(ColumnPath.get(columnDescriptor.getPath()));
  }
}
