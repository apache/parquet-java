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
package org.apache.parquet.hadoop.thrift;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.parquet.column.statistics.*;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.thrift.ParquetWriteProtocol;
import org.apache.parquet.thrift.test.RequiredPrimitiveFixture;
import org.apache.parquet.thrift.test.TestListsInMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.TestUtils;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestThriftToParquetFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TestThriftToParquetFileWriter.class);

  @Test
  public void testWriteStatistics() throws Exception {
    //create correct stats small numbers
    IntStatistics intStatsSmall = new IntStatistics();
    intStatsSmall.setMinMax(2, 100);
    LongStatistics longStatsSmall = new LongStatistics();
    longStatsSmall.setMinMax(-17l, 287L);
    DoubleStatistics doubleStatsSmall = new DoubleStatistics();
    doubleStatsSmall.setMinMax(-15.55d, 9.63d);
    BinaryStatistics binaryStatsSmall = new BinaryStatistics();
    binaryStatsSmall.setMinMax(Binary.fromString("as"), Binary.fromString("world"));
    BooleanStatistics boolStats = new BooleanStatistics();
    boolStats.setMinMax(false, true);

    //write rows to a file
    Path p = createFile(new Configuration(),
      new RequiredPrimitiveFixture(false, (byte) 32, (short) 32, 2, 90l, -15.55d, "as"),
      new RequiredPrimitiveFixture(false, (byte) 100, (short) 100, 100, 287l, -9.0d, "world"),
      new RequiredPrimitiveFixture(true, (byte) 2, (short) 2, 9, -17l, 9.63d, "hello"));
    final Configuration configuration = new Configuration();
    configuration.setBoolean("parquet.strings.signed-min-max.enabled", true);
    final FileSystem fs = p.getFileSystem(configuration);
    FileStatus fileStatus = fs.getFileStatus(p);
    ParquetMetadata footer = ParquetFileReader.readFooter(configuration, p);
    for (BlockMetaData bmd : footer.getBlocks()) {
      for (ColumnChunkMetaData cmd : bmd.getColumns()) {
        switch (cmd.getType()) {
          case INT32:
            TestUtils.assertStatsValuesEqual(intStatsSmall, cmd.getStatistics());
            break;
          case INT64:
            TestUtils.assertStatsValuesEqual(longStatsSmall, cmd.getStatistics());
            break;
          case DOUBLE:
            TestUtils.assertStatsValuesEqual(doubleStatsSmall, cmd.getStatistics());
            break;
          case BOOLEAN:
            TestUtils.assertStatsValuesEqual(boolStats, cmd.getStatistics());
            break;
          case BINARY:
            // there is also info_string that has no statistics
            if (cmd.getPath().toString() == "[test_string]")
              TestUtils.assertStatsValuesEqual(binaryStatsSmall, cmd.getStatistics());
            break;
        }
      }
    }
    //create correct stats large numbers
    IntStatistics intStatsLarge = new IntStatistics();
    intStatsLarge.setMinMax(-Integer.MAX_VALUE, Integer.MAX_VALUE);
    LongStatistics longStatsLarge = new LongStatistics();
    longStatsLarge.setMinMax(-Long.MAX_VALUE, Long.MAX_VALUE);
    DoubleStatistics doubleStatsLarge = new DoubleStatistics();
    doubleStatsLarge.setMinMax(-Double.MAX_VALUE, Double.MAX_VALUE);
    BinaryStatistics binaryStatsLarge = new BinaryStatistics();
    binaryStatsLarge.setMinMax(Binary.fromString("some small string"),
      Binary.fromString("some very large string here to test in this function"));
    //write rows to a file
    Path p_large = createFile(new Configuration(),
      new RequiredPrimitiveFixture(false, (byte) 2, (short) 32, -Integer.MAX_VALUE,
        -Long.MAX_VALUE, -Double.MAX_VALUE, "some small string"),
      new RequiredPrimitiveFixture(false, (byte) 100, (short) 100, Integer.MAX_VALUE,
        Long.MAX_VALUE, Double.MAX_VALUE,
        "some very large string here to test in this function"),
      new RequiredPrimitiveFixture(true, (byte) 2, (short) 2, 9, -17l, 9.63d, "hello"));

    // make new configuration and create file with new large stats
    final Configuration configuration_large = new Configuration();
    configuration.setBoolean("parquet.strings.signed-min-max.enabled", true);
    final FileSystem fs_large = p_large.getFileSystem(configuration_large);
    FileStatus fileStatus_large = fs_large.getFileStatus(p_large);
    ParquetMetadata footer_large = ParquetFileReader.readFooter(configuration_large, p_large);
    for (BlockMetaData bmd : footer_large.getBlocks()) {
      for (ColumnChunkMetaData cmd : bmd.getColumns()) {
        switch (cmd.getType()) {
          case INT32:
            // testing the correct limits of an int32, there are also byte and short, tested earlier
            if (cmd.getPath().toString() == "[test_i32]")
              TestUtils.assertStatsValuesEqual(intStatsLarge, cmd.getStatistics());
            break;
          case INT64:
            TestUtils.assertStatsValuesEqual(longStatsLarge, cmd.getStatistics());
            break;
          case DOUBLE:
            TestUtils.assertStatsValuesEqual(doubleStatsLarge, cmd.getStatistics());
            break;
          case BOOLEAN:
            TestUtils.assertStatsValuesEqual(boolStats, cmd.getStatistics());
            break;
          case BINARY:
            // there is also info_string that has no statistics
            if (cmd.getPath().toString() == "[test_string]")
              TestUtils.assertStatsValuesEqual(binaryStatsLarge, cmd.getStatistics());
            break;
        }
      }
    }
  }

  @Test
  public void testWriteFileMapOfLists() throws IOException, InterruptedException, TException {
    Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
    map.put(Arrays.asList("key1", "key2"), Arrays.asList("val1", "val2"));
    final TestListsInMap mapList = new TestListsInMap("maplists", map);
    final Path fileToCreate = createFile(new Configuration(), mapList);

    ParquetReader<Group> reader = createRecordReader(fileToCreate);

    Group g = null;
    while ((g = reader.read()) != null) {
      assertEquals("key1",
        g.getGroup("names", 0).getGroup("key_value", 0).getGroup("key", 0).getBinary("key_tuple", 0).toStringUsingUTF8());
      assertEquals("key2",
        g.getGroup("names", 0).getGroup("key_value", 0).getGroup("key", 0).getBinary("key_tuple", 1).toStringUsingUTF8());
      assertEquals("val1",
        g.getGroup("names", 0).getGroup("key_value", 0).getGroup("value", 0).getBinary("value_tuple", 0).toStringUsingUTF8());
      assertEquals("val2",
        g.getGroup("names", 0).getGroup("key_value", 0).getGroup("value", 0).getBinary("value_tuple", 1).toStringUsingUTF8());
    }
  }

  private ParquetReader<Group> createRecordReader(Path parquetFilePath) throws IOException {
    Configuration configuration = new Configuration(true);

    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();

    readSupport.init(configuration, null, schema);
    return new ParquetReader<Group>(parquetFilePath, readSupport);
  }

  private <T extends TBase<?, ?>> Path createFile(Configuration conf, T... tObjs)
    throws IOException, InterruptedException, TException {
    final Path fileToCreate = new Path("target/test/TestThriftToParquetFileWriter/" + tObjs[0].getClass() + ".parquet");
    LOG.info("File created: " + fileToCreate.toString());
    final FileSystem fs = fileToCreate.getFileSystem(conf);
    if (fs.exists(fileToCreate)) {
      fs.delete(fileToCreate, true);

    }
    TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
    TaskAttemptID taskId = new TaskAttemptID("local", 0, true, 0, 0);
    try (ThriftToParquetFileWriter w = new ThriftToParquetFileWriter(fileToCreate, ContextUtil.newTaskAttemptContext(conf, taskId), protocolFactory, (Class<? extends TBase<?, ?>>) tObjs[0].getClass())) {
      for (T tObj : tObjs) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));

        tObj.write(protocol);

        w.write(new BytesWritable(baos.toByteArray()));
      }
    }

    return fileToCreate;
  }
}
