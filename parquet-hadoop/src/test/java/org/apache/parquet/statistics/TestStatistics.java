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

package org.apache.parquet.statistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertTrue;

public class TestStatistics {
  private static final int MEGABYTE = 1 << 20;
  private static final long RANDOM_SEED = 1441990701846L; //System.currentTimeMillis();

  public static class DataGenerationContext {
    public static abstract class WriteContext {
      protected final File path;
      protected final Path fsPath;
      protected final MessageType schema;
      protected final int blockSize;
      protected final int pageSize;
      protected final boolean enableDictionary;
      protected final boolean enableValidation;
      protected final ParquetProperties.WriterVersion version;

      public WriteContext(File path, MessageType schema, int blockSize, int pageSize, boolean enableDictionary, boolean enableValidation, ParquetProperties.WriterVersion version) throws IOException {
        this.path = path;
        this.fsPath = new Path(path.toString());
        this.schema = schema;
        this.blockSize = blockSize;
        this.pageSize = pageSize;
        this.enableDictionary = enableDictionary;
        this.enableValidation = enableValidation;
        this.version = version;
      }

      public abstract void write(ParquetWriter<Group> writer) throws IOException;
      public abstract void test() throws IOException;
    }

    public static void writeAndTest(WriteContext context) throws IOException {
      // Create the configuration, and then apply the schema to our configuration.
      Configuration configuration = new Configuration();
      GroupWriteSupport.setSchema(context.schema, configuration);
      GroupWriteSupport groupWriteSupport = new GroupWriteSupport();

      // Create the writer properties
      final int blockSize = context.blockSize;
      final int pageSize = context.pageSize;
      final int dictionaryPageSize = pageSize;
      final boolean enableDictionary = context.enableDictionary;
      final boolean enableValidation = context.enableValidation;
      ParquetProperties.WriterVersion writerVersion = context.version;
      CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

      ParquetWriter<Group> writer = new ParquetWriter<Group>(context.fsPath,
          groupWriteSupport, codec, blockSize, pageSize, dictionaryPageSize,
          enableDictionary, enableValidation, writerVersion, configuration);

      context.write(writer);
      writer.close();

      context.test();

      context.path.delete();
    }
  }

  public static class SingletonPageReader implements PageReader {
    private final DictionaryPage dict;
    private final DataPage data;

    public SingletonPageReader(DictionaryPage dict, DataPage data) {
      this.dict = dict;
      this.data = data;
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      return dict;
    }

    @Override
    public long getTotalValueCount() {
      return data.getValueCount();
    }

    @Override
    public DataPage readPage() {
      return data;
    }
  }

  private static <T extends Comparable<T>> Statistics<T> getStatisticsFromPageHeader(DataPage page) {
    return page.accept(new DataPage.Visitor<Statistics<T>>() {
      @Override
      @SuppressWarnings("unchecked")
      public Statistics<T> visit(DataPageV1 dataPageV1) {
        return (Statistics<T>) dataPageV1.getStatistics();
      }

      @Override
      @SuppressWarnings("unchecked")
      public Statistics<T> visit(DataPageV2 dataPageV2) {
        return (Statistics<T>) dataPageV2.getStatistics();
      }
    });
  }

  private static class StatsValidator<T extends Comparable<T>> {
    private final boolean hasNonNull;
    private final T min;
    private final T max;

    public StatsValidator(DataPage page) {
      Statistics<T> stats = getStatisticsFromPageHeader(page);
      this.hasNonNull = stats.hasNonNullValue();
      if (hasNonNull) {
        this.min = stats.genericGetMin();
        this.max = stats.genericGetMax();
      } else {
        this.min = null;
        this.max = null;
      }
    }

    public void validate(T value) {
      if (hasNonNull) {
        assertTrue("min should be <= all values", min.compareTo(value) <= 0);
        assertTrue("min should be >= all values", max.compareTo(value) >= 0);
      }
    }
  }

  private static PrimitiveConverter getValidatingConverter(
      final DataPage page, PrimitiveTypeName type) {
    return type.convert(new PrimitiveType.PrimitiveTypeNameConverter<PrimitiveConverter, RuntimeException>() {
      @Override
      public PrimitiveConverter convertFLOAT(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Float> validator = new StatsValidator<Float>(page);
        return new PrimitiveConverter() {
          @Override
          public void addFloat(float value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertDOUBLE(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Double> validator = new StatsValidator<Double>(page);
        return new PrimitiveConverter() {
          @Override
          public void addDouble(double value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertINT32(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Integer> validator = new StatsValidator<Integer>(page);
        return new PrimitiveConverter() {
          @Override
          public void addInt(int value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertINT64(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Long> validator = new StatsValidator<Long>(page);
        return new PrimitiveConverter() {
          @Override
          public void addLong(long value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertBOOLEAN(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Boolean> validator = new StatsValidator<Boolean>(page);
        return new PrimitiveConverter() {
          @Override
          public void addBoolean(boolean value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertINT96(PrimitiveTypeName primitiveTypeName) {
        return convertBINARY(primitiveTypeName);
      }

      @Override
      public PrimitiveConverter convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) {
        return convertBINARY(primitiveTypeName);
      }

      @Override
      public PrimitiveConverter convertBINARY(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Binary> validator = new StatsValidator<Binary>(page);
        return new PrimitiveConverter() {
          @Override
          public void addBinary(Binary value) {
            validator.validate(value);
          }
        };
      }
    });
  }

  public static class PageStatsValidator {
    public void validate(MessageType schema, PageReadStore store) {
      for (ColumnDescriptor desc : schema.getColumns()) {
        PageReader reader = store.getPageReader(desc);
        DictionaryPage dict = reader.readDictionaryPage();
        DataPage page;
        while ((page = reader.readPage()) != null) {
          validateStatsForPage(page, dict, desc);
        }
      }
    }

    private void validateStatsForPage(DataPage page, DictionaryPage dict, ColumnDescriptor desc) {
      SingletonPageReader reader = new SingletonPageReader(dict, page);
      PrimitiveConverter converter = getValidatingConverter(page, desc.getType());
      Statistics stats = getStatisticsFromPageHeader(page);

      long numNulls = 0;
      ColumnReaderImpl column = new ColumnReaderImpl(desc, reader, converter, null);
      for (int i = 0; i < reader.getTotalValueCount(); i += 1) {
        if (column.getCurrentDefinitionLevel() >= desc.getMaxDefinitionLevel()) {
          column.writeCurrentValueToConverter();
        } else {
          numNulls += 1;
        }
        column.consume();
      }

      Assert.assertEquals(numNulls, stats.getNumNulls());

      System.err.println(String.format(
          "Validated stats min=%s max=%s nulls=%d for page=%s col=%s",
          String.valueOf(stats.genericGetMin()),
          String.valueOf(stats.genericGetMax()), stats.getNumNulls(), page,
          Arrays.toString(desc.getPath())));
    }
  }

  public static class DataContext extends DataGenerationContext.WriteContext {
    private static final int MAX_TOTAL_ROWS = 1000000;

    private final long seed;
    private final Random random;
    private final int recordCount;

    private final int fixedLength;
    private final RandomValues.IntGenerator intGenerator;
    private final RandomValues.LongGenerator longGenerator;
    private final RandomValues.Int96Generator int96Generator;
    private final RandomValues.FloatGenerator floatGenerator;
    private final RandomValues.DoubleGenerator doubleGenerator;
    private final RandomValues.StringGenerator stringGenerator;
    private final RandomValues.BinaryGenerator binaryGenerator;
    private final RandomValues.FixedGenerator fixedBinaryGenerator;

    public DataContext(long seed, File path, int blockSize, int pageSize, boolean enableDictionary, ParquetProperties.WriterVersion version) throws IOException {
      super(path, buildSchema(seed), blockSize, pageSize, enableDictionary, true, version);

      this.seed = seed;
      this.random = new Random(seed);
      this.recordCount = random.nextInt(MAX_TOTAL_ROWS);

      this.fixedLength = schema.getType("fixed-binary").asPrimitiveType().getTypeLength();
      this.intGenerator = new RandomValues.IntGenerator(random.nextLong());
      this.longGenerator = new RandomValues.LongGenerator(random.nextLong());
      this.int96Generator = new RandomValues.Int96Generator(random.nextLong());
      this.floatGenerator = new RandomValues.FloatGenerator(random.nextLong());
      this.doubleGenerator = new RandomValues.DoubleGenerator(random.nextLong());
      this.stringGenerator = new RandomValues.StringGenerator(random.nextLong());
      this.binaryGenerator = new RandomValues.BinaryGenerator(random.nextLong());
      this.fixedBinaryGenerator = new RandomValues.FixedGenerator(random.nextLong(), fixedLength);
    }

    private static MessageType buildSchema(long seed) {
      Random random = new Random(seed);
      int fixedBinaryLength = random.nextInt(21) + 1;

      return new MessageType("schema",
        new PrimitiveType(OPTIONAL, INT32, "i32"),
        new PrimitiveType(OPTIONAL, INT64, "i64"),
        new PrimitiveType(OPTIONAL, INT96, "i96"),
        new PrimitiveType(OPTIONAL, FLOAT, "sngl"),
        new PrimitiveType(OPTIONAL, DOUBLE, "dbl"),
        new PrimitiveType(OPTIONAL, BINARY, "strings"),
        new PrimitiveType(OPTIONAL, BINARY, "binary"),
        new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, fixedBinaryLength, "fixed-binary"),
        new PrimitiveType(REQUIRED, INT32, "unconstrained-i32"),
        new PrimitiveType(REQUIRED, INT64, "unconstrained-i64"),
        new PrimitiveType(REQUIRED, FLOAT, "unconstrained-sngl"),
        new PrimitiveType(REQUIRED, DOUBLE, "unconstrained-dbl")
      );
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < recordCount; index++) {
        Group group = new SimpleGroup(super.schema);

        if (!intGenerator.shouldGenerateNull()) {
          group.append("i32", intGenerator.nextValue());
        }
        if (!longGenerator.shouldGenerateNull()) {
          group.append("i64", longGenerator.nextValue());
        }
        if (!int96Generator.shouldGenerateNull()) {
          group.append("i96", int96Generator.nextBinaryValue());
        }
        if (!floatGenerator.shouldGenerateNull()) {
          group.append("sngl", floatGenerator.nextValue());
        }
        if (!doubleGenerator.shouldGenerateNull()) {
          group.append("dbl", doubleGenerator.nextValue());
        }
        if (!stringGenerator.shouldGenerateNull()) {
          group.append("strings", stringGenerator.nextBinaryValue());
        }
        if (!binaryGenerator.shouldGenerateNull()) {
          group.append("binary", binaryGenerator.nextBinaryValue());
        }
        if (!fixedBinaryGenerator.shouldGenerateNull()) {
          group.append("fixed-binary", fixedBinaryGenerator.nextBinaryValue());
        }
        group.append("unconstrained-i32", random.nextInt());
        group.append("unconstrained-i64", random.nextLong());
        group.append("unconstrained-sngl", random.nextFloat());
        group.append("unconstrained-dbl", random.nextDouble());

        writer.write(group);
      }
    }

    @Override
    public void test() throws IOException {
      Configuration configuration = new Configuration();
      ParquetMetadata metadata = ParquetFileReader.readFooter(configuration,
          super.fsPath, ParquetMetadataConverter.NO_FILTER);
      ParquetFileReader reader = new ParquetFileReader(configuration,
        metadata.getFileMetaData(),
        super.fsPath,
        metadata.getBlocks(),
        metadata.getFileMetaData().getSchema().getColumns());

      PageStatsValidator validator = new PageStatsValidator();

      PageReadStore pageReadStore;
      while ((pageReadStore = reader.readNextRowGroup()) != null) {
        validator.validate(metadata.getFileMetaData().getSchema(), pageReadStore);
      }
    }
  }

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testStatistics() throws IOException {
    File file = folder.newFile("test_file.parquet");
    file.delete();

    System.out.println(String.format("RANDOM SEED: %s", RANDOM_SEED));

    Random random = new Random(RANDOM_SEED);

    int blockSize =(random.nextInt(54) + 10) * MEGABYTE;
    int pageSize = (random.nextInt(10) + 1) * MEGABYTE;

    List<DataContext> contexts = Arrays.asList(
        new DataContext(random.nextLong(), file, blockSize,
            pageSize, false, ParquetProperties.WriterVersion.PARQUET_1_0),
        new DataContext(random.nextLong(), file, blockSize,
            pageSize, true, ParquetProperties.WriterVersion.PARQUET_1_0),
        new DataContext(random.nextLong(), file, blockSize,
            pageSize, false, ParquetProperties.WriterVersion.PARQUET_2_0),
        new DataContext(random.nextLong(), file, blockSize,
            pageSize, true, ParquetProperties.WriterVersion.PARQUET_2_0)
    );

    for (DataContext test : contexts) {
      DataGenerationContext.writeAndTest(test);
    }
  }
}
