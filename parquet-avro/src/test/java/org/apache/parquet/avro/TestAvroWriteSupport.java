/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.avro;

import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

public class TestAvroWriteSupport {

  @Test
  public void testInitConfiguration() throws IOException {
    final String listField = "NullableList";
    Schema schema = new Schema.Parser()
        .parse(Resources.getResource("list_with_nulls.avsc").openStream());
    AvroWriteSupport<GenericRecord> awsWithConfigSpecificBehavior = new AvroWriteSupport<>();
    Configuration configuration = new Configuration();
    AvroWriteSupport.setSchema(configuration, schema);
    configuration.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false");
    WriteSupport.WriteContext writeContext = awsWithConfigSpecificBehavior.init(configuration);
    MessageType actual = writeContext.getSchema();
    int idx = actual.getFieldIndex(listField);
    ColumnDescriptor columnDescriptor = actual.getColumns().get(idx);
    PrimitiveType primitiveType = columnDescriptor.getPrimitiveType();
    Assert.assertEquals(
        "Confirm that old list type was not used", AvroWriteSupport.LIST_ELEMENT_NAME, primitiveType.getName());
    // Default configuration
    configuration = new Configuration();
    AvroWriteSupport.setSchema(configuration, schema);
    AvroWriteSupport<GenericRecord> awsWithoutConfigBehavior = new AvroWriteSupport<>();
    WriteSupport.WriteContext writeContextDefault = awsWithoutConfigBehavior.init(configuration);
    actual = writeContextDefault.getSchema();
    idx = actual.getFieldIndex(listField);
    columnDescriptor = actual.getColumns().get(idx);
    primitiveType = columnDescriptor.getPrimitiveType();
    Assert.assertEquals("Confirm that old list type was used if not specified", "array", primitiveType.getName());
  }
}
