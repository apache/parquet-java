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

import org.apache.parquet.column.values.factory.ValuesWriterFactory;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotSame;

public class ParquetPropertiesThreadSafetyTest {

  @Test
  public void testBuilderValuesWriterFactoryNotShared() throws Exception {
    ParquetProperties.Builder builder1 = ParquetProperties.builder();
    ParquetProperties.Builder builder2 = ParquetProperties.builder();
    ParquetProperties.Builder builder3 = ParquetProperties.builder();

    Field factoryField = ParquetProperties.Builder.class.getDeclaredField("valuesWriterFactory");
    factoryField.setAccessible(true);

    ValuesWriterFactory factory1 = (ValuesWriterFactory) factoryField.get(builder1);
    ValuesWriterFactory factory2 = (ValuesWriterFactory) factoryField.get(builder2);
    ValuesWriterFactory factory3 = (ValuesWriterFactory) factoryField.get(builder3);

    assertNotSame("Builder instances should not share ValuesWriterFactory instances", factory1, factory2);
    assertNotSame("Builder instances should not share ValuesWriterFactory instances", factory2, factory3);
    assertNotSame("Builder instances should not share ValuesWriterFactory instances", factory1, factory3);
  }
}
