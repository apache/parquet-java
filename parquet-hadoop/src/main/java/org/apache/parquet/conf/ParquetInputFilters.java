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

import java.io.IOException;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.SerializationUtil;

/**
 * Resolves the filter used to read Parquet files from a {@link ParquetConfiguration}.
 * This class is decoupled from the legacy Hadoop {@code Configuration} and
 * {@code InputFormat} machinery, so that the filter resolution logic can be used
 * without loading {@code FileInputFormat} and its transitive dependencies.
 */
public final class ParquetInputFilters {

  private ParquetInputFilters() {}

  /**
   * @param configuration a configuration
   * @return an unbound record filter class
   */
  public static Class<?> getUnboundRecordFilter(ParquetConfiguration configuration) {
    return ConfigurationUtil.getClassFromConfig(
        configuration, ParquetInputProperties.UNBOUND_RECORD_FILTER, UnboundRecordFilter.class);
  }

  /**
   * @param configuration a configuration
   * @return the filter predicate stored in the configuration, or null if none is set
   */
  public static FilterPredicate getFilterPredicate(ParquetConfiguration configuration) {
    try {
      return SerializationUtil.readObjectFromConfAsBase64(ParquetInputProperties.FILTER_PREDICATE, configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static UnboundRecordFilter getUnboundRecordFilterInstance(ParquetConfiguration configuration) {
    Class<?> clazz = getUnboundRecordFilter(configuration);
    if (clazz == null) {
      return null;
    }
    try {
      return (UnboundRecordFilter) clazz.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate unbound record filter class", e);
    }
  }

  /**
   * Returns a non-null Filter, which is a wrapper around either a
   * FilterPredicate, an UnboundRecordFilter, or a no-op filter.
   *
   * @param conf a configuration
   * @return a filter for the unbound record filter specified in conf
   */
  public static Filter getFilter(ParquetConfiguration conf) {
    return FilterCompat.get(getFilterPredicate(conf), getUnboundRecordFilterInstance(conf));
  }
}
