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
import org.apache.parquet.column.values.factory.DefaultValuesWriterFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestParquetOutputFormatFactoryOverrides {

  @Test
  public void testMissingOverrideParam() {
    Configuration conf = new Configuration();
    Class<?> factoryOverride = ParquetOutputFormat.getFactoryOverride(conf);
    assertEquals("Incorrect default factory override", DefaultValuesWriterFactory.class, factoryOverride);
  }

  @Test
  public void testMissingFactoryOverrideClass() {
    Configuration conf = new Configuration();
    conf.set(ParquetOutputFormat.WRITER_FACTORY_OVERRIDE, "MyMissingFactory");
    Class<?> factoryOverride = ParquetOutputFormat.getFactoryOverride(conf);
    assertEquals("Incorrect default factory override", DefaultValuesWriterFactory.class, factoryOverride);
  }

  @Test
  public void testFactoryClassIncorrectInterface() {
    Configuration conf = new Configuration();
    conf.set(ParquetOutputFormat.WRITER_FACTORY_OVERRIDE, "org.apache.parquet.column.values.factory.ValuesWriterFactoryParams");
    Class<?> factoryOverride = ParquetOutputFormat.getFactoryOverride(conf);
    assertEquals("Incorrect default factory override", DefaultValuesWriterFactory.class, factoryOverride);
  }

  @Test
  public void testValidFactoryOverride() {
    Configuration conf = new Configuration();
    conf.set(ParquetOutputFormat.WRITER_FACTORY_OVERRIDE, "org.apache.parquet.hadoop.StubValuesWriterFactory");
    Class<?> factoryOverride = ParquetOutputFormat.getFactoryOverride(conf);
    assertEquals("Incorrect factory override chosen", StubValuesWriterFactory.class, factoryOverride);
  }
}
