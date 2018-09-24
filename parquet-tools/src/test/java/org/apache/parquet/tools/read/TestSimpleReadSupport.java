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
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.DirectWriterTest;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.Binary;

public class TestSimpleReadSupport extends DirectWriterTest{
    private final String SCHEMA = "message RepeatedPrimitiveInList {"
         +   "required group my_list (LIST) {"
         +     "repeated group list {"
         +       "optional int32 element;"
         +     "}"
         +   "}"
         +   "required group A {"
         +     "optional int32 B;"
         +   "}"
         +   "optional binary bar (UTF8); "
         +   "optional binary foo (UTF8); "
         + "}";

    @Test
    public void testSimpleRead() throws java.io.IOException {
        // write a simple parquet file
        Path path = writeDirect(
        SCHEMA,
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
                rc.startField("my_list", 0);
                    rc.startGroup();
                        rc.startField("list", 0);
                            rc.startField("element", 0);
                                rc.addInteger(34);
                                rc.addInteger(35);
                            rc.endField("element", 0);
                        rc.endField("list", 0);
                    rc.endGroup();
                rc.endField("my_list", 0);

                rc.startField("A", 1);
                    rc.startGroup();
                        rc.startField("B", 0);
                        rc.addInteger(21);
                        rc.endField("B", 0);
                    rc.endGroup();
                rc.endField("A", 1);

                rc.startField("bar", 2);
                rc.addBinary(Binary.fromCharSequence("one"));
                rc.endField("bar", 2);

                rc.startField("foo", 3);
                rc.addBinary(Binary.fromCharSequence("two"));
                rc.endField("foo", 3);

            rc.endMessage();

          }
        });

        ParquetReader reader = ParquetReader.builder(new SimpleReadSupport(false), path).build();
        SimpleRecord r = (SimpleRecord) reader.read();

        // test the regular output
        StringWriter sw = new StringWriter();
        PrintWriter printer = new PrintWriter(sw);
        r.prettyPrint(printer,0);
        String output = sw.toString();

        Assert.assertTrue(output.contains(".B = 21"));
        Assert.assertTrue(output.contains("..element = 34"));
        Assert.assertTrue(output.contains("..element = 35"));
        Assert.assertTrue(output.contains("bar = one"));
        Assert.assertTrue(output.contains("foo = two"));

        // test the json output
        sw = new StringWriter();
        printer = new PrintWriter(sw);
        r.prettyPrintJson(printer);
        String jsonOutput = sw.toString();
        System.out.println(jsonOutput);

        Assert.assertTrue(jsonOutput.contains("B\":21"));
        Assert.assertTrue(jsonOutput.contains("element\":34"));
        Assert.assertTrue(jsonOutput.contains("element\":35"));
        Assert.assertTrue(jsonOutput.contains("bar\":\"one"));
        Assert.assertTrue(jsonOutput.contains("foo\":\"two"));
    }

    @Test
    public void testNullValues() throws java.io.IOException {
        // write a simple parquet file
        Path path = writeDirect(
        SCHEMA,
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.endMessage();
          }
        });

        ParquetReader reader = ParquetReader.builder(new SimpleReadSupport(true), path).build();
        SimpleRecord r = (SimpleRecord) reader.read();

        // test the regular output
        StringWriter sw = new StringWriter();
        PrintWriter printer = new PrintWriter(sw);
        r.prettyPrint(printer,0);

        Assert.assertTrue(sw.toString().contains("foo = <null>"));
        Assert.assertTrue(sw.toString().contains("B = <null>"));
        Assert.assertTrue(sw.toString().contains("list = <null>"));

        // test the json output
        sw = new StringWriter();
        printer = new PrintWriter(sw);
        r.prettyPrintJson(printer);
        Assert.assertTrue(sw.toString().contains("foo\":null"));
        Assert.assertTrue(sw.toString().contains("B\":null"));
        Assert.assertTrue(sw.toString().contains("list\":null"));
    }

    @Test
    public void testNoNullValues() throws java.io.IOException {
        // write a simple parquet file
        Path path = writeDirect(
        SCHEMA,
        new DirectWriter() {
          @Override
          public void write(RecordConsumer rc) {
            rc.startMessage();
            rc.endMessage();
          }
        });

        ParquetReader reader = ParquetReader.builder(new SimpleReadSupport(false), path).build();
        SimpleRecord r = (SimpleRecord) reader.read();

        StringWriter sw = new StringWriter();
        PrintWriter printer = new PrintWriter(sw);
        r.prettyPrint(printer,0);

        Assert.assertTrue(!sw.toString().contains("foo = <null>"));
        Assert.assertTrue(!sw.toString().contains("B = <null>"));
        Assert.assertTrue(!sw.toString().contains("list = <null>"));

        // test the json output
        sw = new StringWriter();
        printer = new PrintWriter(sw);
        r.prettyPrintJson(printer);
        Assert.assertTrue(!sw.toString().contains("foo\":null"));
        Assert.assertTrue(!sw.toString().contains("B\":null"));
        Assert.assertTrue(!sw.toString().contains("list\":null"));
    }
}
