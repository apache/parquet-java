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
package org.apache.parquet.tools.read;


import java.io.StringWriter;
import java.io.PrintWriter;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class TestSimpleReadSupport {
    @Test
    public void testNullValues() throws java.io.IOException {
        Path path = new Path("target/tests/TestSimpleRecordConverter/");
        Configuration conf = new Configuration();
        try {
            // write a simple parquet file
            MessageType schema = parseMessageType(
            "message test { "
            + "optional fixed_len_byte_array(3) bar; "
            + "optional fixed_len_byte_array(3) foo; "
            + "} ");
            GroupWriteSupport.setSchema(schema, conf);
            SimpleGroupFactory f = new SimpleGroupFactory(schema);

            ParquetWriter<Group> writer = new ParquetWriter<Group>(path, conf, new GroupWriteSupport());

            writer.write(f.newGroup().append("bar", "bak"));
            writer.close();

            ParquetReader reader = ParquetReader.builder(new SimpleReadSupport(true), path).build();
            SimpleRecord r = (SimpleRecord) reader.read();

            // test the regular output
            StringWriter sw = new StringWriter();
            PrintWriter printer = new PrintWriter(sw);
            r.prettyPrint(printer, 0);
            Assert.assertTrue(sw.toString().contains("foo = <null>"));

            // test the json output
            sw = new StringWriter();
            printer = new PrintWriter(sw);
            r.prettyPrintJson(printer);
            Assert.assertTrue(sw.toString().contains("foo\":null"));
        } finally {
            path.getFileSystem(conf).delete(path, false);
        }

    }
}
