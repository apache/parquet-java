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
import org.apache.parquet.column.values.factory.ConfigurableFactory;
import org.apache.parquet.column.values.factory.DefaultValuesWriterFactory;
import org.apache.parquet.column.values.factory.ValuesWriterFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestParquetOutputFormatFactoryOverrides {

  @Test
  public void testMissingOverrideParam() {
    Configuration conf = new Configuration();
    ValuesWriterFactory factory = ParquetOutputFormat.getValuesWriterFactory(conf);
    assertEquals("Incorrect default factory override", DefaultValuesWriterFactory.class, factory.getClass());
  }

  @Test(expected = BadConfigurationException.class)
  public void testMissingFactoryOverrideClass() {
    Configuration conf = new Configuration();
    conf.set(ParquetOutputFormat.WRITER_FACTORY_OVERRIDE, "MyMissingFactory");

    // should throw as we can't find MyMissingFactory
    ParquetOutputFormat.getValuesWriterFactory(conf);
  }

  @Test(expected = BadConfigurationException.class)
  public void testFactoryClassIncorrectInterface() {
    Configuration conf = new Configuration();
    conf.set(ParquetOutputFormat.WRITER_FACTORY_OVERRIDE, "org.apache.parquet.column.values.factory.ValuesWriterFactoryParams");

    // should throw as ValuesWriterFactoryParams isn't implementing ValuesWriterFactory
    ParquetOutputFormat.getValuesWriterFactory(conf);
  }

  @Test
  public void testValidFactoryOverride() {
    Configuration conf = new Configuration();
    conf.set(ParquetOutputFormat.WRITER_FACTORY_OVERRIDE, "org.apache.parquet.hadoop.StubValuesWriterFactory");
    ValuesWriterFactory factory = ParquetOutputFormat.getValuesWriterFactory(conf);
    assertEquals("Incorrect factory override chosen", StubValuesWriterFactory.class, factory.getClass());
  }

  @Test
  public void testFactoryOverrideWithCfg() {
    Configuration conf = new Configuration();
    conf.set("foo", "bar");
    conf.set(ParquetOutputFormat.WRITER_FACTORY_OVERRIDE, "org.apache.parquet.hadoop.StubConfigurableValuesWriterFactory");

    ValuesWriterFactory factory = ParquetOutputFormat.getValuesWriterFactory(conf);
    assertEquals("Incorrect factory override chosen", StubConfigurableValuesWriterFactory.class, factory.getClass());

    ConfigurableFactory configurable = (ConfigurableFactory) factory;
    assertNotNull("Not a ConfigurableFactory", configurable);
    assertEquals("Incorrect config value found", "bar", configurable.getConfiguration().get("foo"));
  }
}
