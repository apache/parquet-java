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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.parquet.hadoop.UnmaterializableRecordCounter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetWriter;
import org.apache.parquet.thrift.test.compat.ABool;
import org.apache.parquet.thrift.test.compat.ALong;
import org.apache.parquet.thrift.test.compat.AString;
import org.apache.parquet.thrift.test.compat.AStructThatLooksLikeUnionV2;
import org.apache.parquet.thrift.test.compat.StructWithAStructThatLooksLikeUnionV2;
import org.apache.parquet.thrift.test.compat.StructWithUnionV2;
import org.apache.parquet.thrift.test.compat.UnionV2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.apache.parquet.hadoop.thrift.TestInputOutputFormat.waitForJob;

public class TestCorruptThriftRecords {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  public static class ReadMapper<T> extends Mapper<Void, T, Void, Void> {
    public static List<Object> records;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      records = new ArrayList<Object>();
    }

    @Override
    protected void map(Void key, T value, Context context) throws IOException, InterruptedException {
      records.add(value);
    }
  }

  public static StructWithAStructThatLooksLikeUnionV2 makeValid(int i) {
    AStructThatLooksLikeUnionV2 validUnion = new AStructThatLooksLikeUnionV2();
    switch (i % 3) {
      case 0:
        validUnion.setALong(new ALong(17L));
        break;
      case 1:
        validUnion.setANewBool(new ABool(false));
        break;
      case 2:
        validUnion.setAString(new AString("bar"));
        break;
    }
    return new StructWithAStructThatLooksLikeUnionV2("foo" + i, validUnion);
  }

  public static StructWithUnionV2 makeExpectedValid(int i) {
    UnionV2 validUnion = new UnionV2();
    switch (i % 3) {
      case 0:
        validUnion.setALong(new ALong(17L));
        break;
      case 1:
        validUnion.setANewBool(new ABool(false));
        break;
      case 2:
        validUnion.setAString(new AString("bar"));
        break;
    }
    return new StructWithUnionV2("foo" + i, validUnion);
  }

  public static StructWithAStructThatLooksLikeUnionV2 makeInvalid(int i) {
    AStructThatLooksLikeUnionV2 invalid = new AStructThatLooksLikeUnionV2();
    if (i % 2 == 0) {
      // sometimes write too many
      invalid.setALong(new ALong(18l));
      invalid.setANewBool(new ABool(false));
    } else {
      // sometimes write too few
    }
    return new StructWithAStructThatLooksLikeUnionV2("foo" + i, invalid);
  }

  protected void setupJob(Job job, Path path) throws Exception {
    job.setInputFormatClass(ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.setInputPaths(job, path);
    ParquetThriftInputFormat.setThriftClass(job.getConfiguration(), StructWithUnionV2.class);

    job.setMapperClass(ReadMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);
  }

  protected void assertEqualsExcepted(List<StructWithUnionV2> expected, List<Object> found) throws Exception {
    assertEquals(expected, found);
  }

  private Path writeFileWithCorruptRecords(int numCorrupt, List<StructWithUnionV2> collectExpectedRecords) throws Exception {
    // generate a file with records that are corrupt according to thrift
    // by writing some structs that when interpreted as unions will be
    // unreadable
    Path outputPath = new Path(new File(tempDir.getRoot(), "corrupt_out").getAbsolutePath());
    ParquetWriter<StructWithAStructThatLooksLikeUnionV2> writer = new ThriftParquetWriter<StructWithAStructThatLooksLikeUnionV2>(
        outputPath,
        StructWithAStructThatLooksLikeUnionV2.class,
        CompressionCodecName.UNCOMPRESSED
    );

    int numRecords = 0;

    for (int i = 0; i < 100; i++) {
      StructWithAStructThatLooksLikeUnionV2 valid  = makeValid(numRecords);
      StructWithUnionV2 expected = makeExpectedValid(numRecords);
      numRecords++;
      collectExpectedRecords.add(expected);
      writer.write(valid);
    }

    for (int i = 0; i < numCorrupt; i++) {
      writer.write(makeInvalid(numRecords++));
    }

    for (int i = 0; i < 100; i++) {
      StructWithAStructThatLooksLikeUnionV2 valid  = makeValid(numRecords);
      StructWithUnionV2 expected = makeExpectedValid(numRecords);
      numRecords++;
      collectExpectedRecords.add(expected);
      writer.write(valid);
    }

    writer.close();

    return outputPath;
  }

  private void readFile(Path path, Configuration conf, String name) throws Exception {
    Job job = new Job(conf, name);
    setupJob(job, path);
    waitForJob(job);
  }

  @Test
  public void testDefaultsToNoTolerance() throws Exception {
    ArrayList<StructWithUnionV2> expected = new ArrayList<StructWithUnionV2>();
    try {
      readFile(writeFileWithCorruptRecords(1, expected), new Configuration(), "testDefaultsToNoTolerance");
      fail("This should throw");
    } catch (RuntimeException e) {
      // still should have actually read all the valid records
      assertEquals(100, ReadMapper.records.size());
      assertEqualsExcepted(expected.subList(0, 100), ReadMapper.records);
    }
  }

  @Test
  public void testCanTolerateBadRecords() throws Exception {
    Configuration conf = new Configuration();
    conf.setFloat(UnmaterializableRecordCounter.BAD_RECORD_THRESHOLD_CONF_KEY, 0.1f);

    List<StructWithUnionV2> expected = new ArrayList<StructWithUnionV2>();

    readFile(writeFileWithCorruptRecords(4, expected), conf, "testCanTolerateBadRecords");
    assertEquals(200, ReadMapper.records.size());
    assertEqualsExcepted(expected, ReadMapper.records);
  }

  @Test
  public void testThrowsWhenTooManyBadRecords() throws Exception {
    Configuration conf = new Configuration();
    conf.setFloat(UnmaterializableRecordCounter.BAD_RECORD_THRESHOLD_CONF_KEY, 0.1f);

    ArrayList<StructWithUnionV2> expected = new ArrayList<StructWithUnionV2>();

    try {
      readFile(writeFileWithCorruptRecords(300, expected), conf, "testThrowsWhenTooManyBadRecords");
      fail("This should throw");
    } catch (RuntimeException e) {
      // still should have actually read all the valid records
      assertEquals(100, ReadMapper.records.size());
      assertEqualsExcepted(expected.subList(0, 100), ReadMapper.records);
    }
  }
}
