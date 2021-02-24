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
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.statistics.DataGenerationContext;
import org.apache.parquet.statistics.RandomValues;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.*;

/**
 * This tests the random access methods of the ParquetFileReader, specifically:
 * <ul>
 *   <li>{@link ParquetFileReader#readRowGroup(int)}</li>
 *   <li>{@link ParquetFileReader#readFilteredRowGroup(int)}</li>
 * </ul>
 *
 *  For this we use two columns.
 *  Column "i64" that starts at value 0 and counts up.
 *  Column "i64_flip" that start at value 1 and flips between 1 and 0.
 *
 *  With these two column we can validate the read data without holding the written data in memory.
 *  The "i64_flip" column is mainly used to test the filtering.
 *  We filter "i64_flip" to be equal to one, that means all values in "i64" have to be even.
 */
public class TestParquetReaderRandomAccess {
  private static final int KILOBYTE = 1 << 10;
  private static final long RANDOM_SEED = 7174252115631550700L;

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void test() throws IOException {
    Random random = new Random(RANDOM_SEED);

    File file = temp.newFile("test_file.parquet");
    file.delete();

    int blockSize = 50 * KILOBYTE;
    int pageSize = 2 * KILOBYTE;

    List<DataContext> contexts = new ArrayList<>();

    for (boolean enableDictionary : new boolean[]{false, true}) {
      for (WriterVersion writerVersion : new WriterVersion[]{WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0}) {
       contexts.add(
         new DataContextRandom(random.nextLong(), file, blockSize,
           pageSize, enableDictionary, writerVersion));
       contexts.add(
         new DataContextRandomAndSequential(random.nextLong(), file, blockSize,
           pageSize, enableDictionary, writerVersion));
      }
    }

    for (DataContext context : contexts) {
      DataGenerationContext.writeAndTest(context);
    }
  }

  public static class SequentialLongGenerator extends RandomValues.RandomValueGenerator<Long> {
    private long value= 0;

    protected SequentialLongGenerator() {
      super(0L);
    }

    @Override
    public Long nextValue() {
      return value++;
    }
  }

  public static class SequentialFlippingLongGenerator extends RandomValues.RandomValueGenerator<Long> {
    private long value = 0;

    protected SequentialFlippingLongGenerator() {
      super(0L);
    }

    @Override
    public Long nextValue() {
      value = value == 0 ? 1 : 0;
      return value;
    }
  }

  public static abstract class DataContext extends DataGenerationContext.WriteContext {

    private static final int recordCount = 1_000_000;

    private final List<RandomValues.RandomValueGenerator<?>> randomGenerators;
    private final Random random;
    private final FilterCompat.Filter filter;

    public DataContext(long seed, File path, int blockSize, int pageSize, boolean enableDictionary, ParquetProperties.WriterVersion version) throws IOException {
      super(path, buildSchema(), blockSize, pageSize, enableDictionary, true, version);

      this.random = new Random(seed);
      this.randomGenerators = Arrays.asList(
        new SequentialLongGenerator(),
        new SequentialFlippingLongGenerator());

      this.filter = FilterCompat.get(eq(longColumn("i64_flip"), 1L));
    }

    private static MessageType buildSchema() {
      return new MessageType("schema",
        new PrimitiveType(REQUIRED, INT64, "i64"),
        new PrimitiveType(REQUIRED, INT64, "i64_flip"));
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < recordCount; index++) {
        Group group = new SimpleGroup(super.schema);

        for (int column = 0, columnCnt = schema.getFieldCount(); column < columnCnt; ++column) {
          Type type = schema.getType(column);
          RandomValues.RandomValueGenerator<?> generator = randomGenerators.get(column);
          if (type.isRepetition(OPTIONAL) && generator.shouldGenerateNull()) {
            continue;
          }
          group.append(type.getName(), (Long) generator.nextValue());
        }
        writer.write(group);
      }
    }

    @Override
    public void test() throws IOException {
      Configuration configuration = new Configuration();
      ParquetReadOptions options = ParquetReadOptions.builder().build();

      ParquetReadOptions filterOptions = ParquetReadOptions.builder()
        .copy(options)
        .withRecordFilter(filter)
        .useDictionaryFilter(true)
        .useStatsFilter(true)
        .useRecordFilter(true)
        .useColumnIndexFilter(true)
        .build();

      List<Long> fromNumber = new ArrayList<>();
      List<Long> toNumber = new ArrayList<>();
      int blocks;

      try (ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(super.fsPath, configuration), options)) {
        blocks = reader.getRowGroups().size();
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(super.schema);
          RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(super.schema));
          long rowCount = pages.getRowCount();
          long from = recordReader.read().getLong("i64", 0);
          for (int i = 1; i < rowCount - 1; i++) {
            recordReader.read();
          }
          Group group = recordReader.read();
          long to;
          if (group == null) {
            to = from;
          } else {
            to = group.getLong("i64", 0);
          }
          fromNumber.add(from);
          toNumber.add(to);
        }
      }

      // Randomize indexes
      List<Integer> indexes = new ArrayList<>();
      for (int i = 0; i < blocks; i++) {
        for (int j = 0; j < 4; j++) {
          indexes.add(i);
        }
      }

      Collections.shuffle(indexes, random);

      try (ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(super.fsPath, configuration), options)) {
        test(reader, indexes, fromNumber, toNumber);
      }

      try (ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(super.fsPath, configuration), filterOptions)) {
        testFiltered(reader, indexes, fromNumber, toNumber);
      }
    }

    public void assertValues(PageReadStore pages, long firstValue, long lastValue) {
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(super.schema);
      RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(super.schema));
      for (long i = firstValue; i <= lastValue; i++) {
        Group group = recordReader.read();
        assertEquals(i, group.getLong("i64", 0));
        assertEquals((i % 2) == 0 ? 1 : 0, group.getLong("i64_flip", 0));
      }
      boolean exceptionThrown = false;
      try {
        recordReader.read();
      } catch (ParquetDecodingException e) {
        exceptionThrown = true;
      }
      assertTrue(exceptionThrown);
    }

    public void assertFilteredValues(PageReadStore pages, long firstValue, long lastValue) {
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(super.schema);
      RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(super.schema), filter);

      for (long i = firstValue; i <= lastValue; i++) {
        Group group = recordReader.read();
        if ((i % 2) == 0) {
          assertEquals(i, group.getLong("i64", 0));
          assertEquals(1, group.getLong("i64_flip", 0));
        } else {
          assertTrue(group == null || recordReader.shouldSkipCurrentRecord());
        }
      }

      boolean exceptionThrown = false;
      try {
        recordReader.read();
      } catch (ParquetDecodingException e) {
        exceptionThrown = true;
      }
      assertTrue(exceptionThrown);
    }

    protected abstract void test(ParquetFileReader reader, List<Integer> indexes, List<Long> fromNumber, List<Long> toNumber) throws IOException;
    protected abstract void testFiltered(ParquetFileReader reader, List<Integer> indexes, List<Long> fromNumber, List<Long> toNumber) throws IOException;
  }

  public static class DataContextRandom extends DataContext {

    public DataContextRandom(long seed, File path, int blockSize, int pageSize, boolean enableDictionary, ParquetProperties.WriterVersion version) throws IOException {
      super(seed, path, blockSize, pageSize, enableDictionary, version);
    }

    @Override
    protected void test(ParquetFileReader reader, List<Integer> indexes, List<Long> fromNumber, List<Long> toNumber) throws IOException {
      for (int index: indexes) {
        PageReadStore pages = reader.readRowGroup(index);
        assertValues(pages, fromNumber.get(index), toNumber.get(index));
      }
    }

    @Override
    protected void testFiltered(ParquetFileReader reader, List<Integer> indexes, List<Long> fromNumber, List<Long> toNumber) throws IOException {
      for (int index: indexes) {
        PageReadStore pages = reader.readFilteredRowGroup(index);
        assertFilteredValues(pages, fromNumber.get(index), toNumber.get(index));
      }
    }
  }

  public static class DataContextRandomAndSequential extends DataContext {

    public DataContextRandomAndSequential(long seed, File path, int blockSize, int pageSize, boolean enableDictionary, ParquetProperties.WriterVersion version) throws IOException {
      super(seed, path, blockSize, pageSize, enableDictionary, version);
    }

    @Override
    protected void test(ParquetFileReader reader, List<Integer> indexes, List<Long> fromNumber, List<Long> toNumber) throws IOException {
      int splitPoint = indexes.size()/2;

      {
        PageReadStore pages = reader.readNextRowGroup();
        assertValues(pages, fromNumber.get(0), toNumber.get(0));
      }
      for (int i = 0; i < splitPoint; i++) {
        int index = indexes.get(i);
        PageReadStore pages = reader.readRowGroup(index);
        assertValues(pages, fromNumber.get(index), toNumber.get(index));
      }
      {
        PageReadStore pages = reader.readNextRowGroup();
        assertValues(pages, fromNumber.get(1), toNumber.get(1));
      }
      for (int i = splitPoint; i < indexes.size(); i++) {
        int index = indexes.get(i);
        PageReadStore pages = reader.readRowGroup(index);
        assertValues(pages, fromNumber.get(index), toNumber.get(index));
      }
      {
        PageReadStore pages = reader.readNextRowGroup();
        assertValues(pages, fromNumber.get(2), toNumber.get(2));
      }
    }

    @Override
    protected void testFiltered(ParquetFileReader reader, List<Integer> indexes, List<Long> fromNumber, List<Long> toNumber) throws IOException {
      int splitPoint = indexes.size()/2;

      {
        PageReadStore pages = reader.readNextFilteredRowGroup();
        assertFilteredValues(pages, fromNumber.get(0), toNumber.get(0));
      }
      for (int i = 0; i < splitPoint; i++) {
        int index = indexes.get(i);
        PageReadStore pages = reader.readFilteredRowGroup(index);
        assertFilteredValues(pages, fromNumber.get(index), toNumber.get(index));
      }
      {
        PageReadStore pages = reader.readNextFilteredRowGroup();
        assertFilteredValues(pages, fromNumber.get(1), toNumber.get(1));
      }
      for (int i = splitPoint; i < indexes.size(); i++) {
        int index = indexes.get(i);
        PageReadStore pages = reader.readFilteredRowGroup(index);
        assertFilteredValues(pages, fromNumber.get(index), toNumber.get(index));
      }
      {
        PageReadStore pages = reader.readNextFilteredRowGroup();
        assertFilteredValues(pages, fromNumber.get(2), toNumber.get(2));
      }
    }
  }

}
