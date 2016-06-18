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
package org.apache.parquet.column.values.factory;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * Can be overridden to allow users to specify how they want their ValuesWriters to be created.
 * ValuesWriterFactories are created using reflection in {@link org.apache.parquet.column.ParquetProperties}.
 * Due to this, they must provide a default constructor.
 * Lifecycle of ValuesWriterFactories is:
 * 1) Created via reflection while creating a {@link org.apache.parquet.column.ParquetProperties}
 * 2) If the factory is Configurable (needs Hadoop conf), that is set, initialize is also called. This is done
 * just once for the lifetime of the factory. As Hadoop config can be set, ValuesWriterFactories can
 * read additional config to create appropriate ValuesWriters.
 * 3) newValuesWriter is called once per column for every block of data.
 *
 */
public interface ValuesWriterFactory {

  /**
   * Used to initialize the factory. This method is called before newValuesWriter()
   */
  void initialize(ValuesWriterFactoryParams params);

  /**
   * Creates a ValuesWriter to help write the given column.
   */
  ValuesWriter newValuesWriter(ColumnDescriptor descriptor);
}
