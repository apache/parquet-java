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
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.junit.Assert.assertTrue;

public class TestStatistics {
  private static final int MEGABYTE = 1 << 20;
  private static final long RANDOM_SEED = System.currentTimeMillis();

  public static class DataGenerationContext {
    public static abstract class WriteContext {
      protected final String path;
      protected final Path fsPath;
      protected final MessageType schema;
      protected final int blockSize;
      protected final int pageSize;
      protected final boolean enableDictionary;
      protected final boolean enableValidation;
      protected final ParquetProperties.WriterVersion version;

      public WriteContext(String path, MessageType schema, int blockSize, int pageSize, boolean enableDictionary, boolean enableValidation, ParquetProperties.WriterVersion version) throws IOException {
        this.path = path;
        this.fsPath = new Path(path);
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
      File file = new File(context.path);

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

      ParquetWriter<Group> writer = new ParquetWriter<Group>(context.fsPath, groupWriteSupport, codec, blockSize,
        pageSize, dictionaryPageSize, enableDictionary, enableValidation, writerVersion, configuration);

      context.write(writer);
      writer.close();

      context.test();
      file.delete();
    }
  }

  public static abstract class PagingValidator<T extends Comparable<T>> {
    private final ColumnDescriptor columnDescriptor;
    private final PrimitiveType.PrimitiveTypeName forType;

    public PagingValidator(ColumnDescriptor columnDescriptor) {
      this.columnDescriptor = columnDescriptor;
      this.forType = columnDescriptor.getType();
    }

    private void validate(PageReadStore store) {
      PageReader reader = store.getPageReader(columnDescriptor);
      DataPage page;
      while ((page = reader.readPage()) != null) {
        validateStatsForPage(page);
      }
    }

    private Statistics<T> getStatisticsFromPageHeader(DataPage page) {
      return page.accept(new DataPage.Visitor<Statistics<T>>() {
        @Override
        public Statistics<T> visit(DataPageV1 dataPageV1) {
          return (Statistics<T>)dataPageV1.getStatistics();
        }

        @Override
        public Statistics<T> visit(DataPageV2 dataPageV2) {
          return (Statistics<T>)dataPageV2.getStatistics();
        }
      });
    }

    private void validateStatsForPage(final DataPage page) {
      page.accept(new DataPage.Visitor<Object>() {
        @Override
        public Object visit(DataPageV1 dataPageV1) {
          try {
            ValuesReader definitionLevelReader = dataPageV1
              .getDlEncoding()
              .getValuesReader(columnDescriptor, ValuesType.DEFINITION_LEVEL);

            definitionLevelReader.initFromPage(dataPageV1.getValueCount(),
              dataPageV1.getBytes().toByteArray(),
              0);

            ValuesReader valuesReader = dataPageV1
              .getValueEncoding()
              .getValuesReader(columnDescriptor, ValuesType.VALUES);

            valuesReader.initFromPage(dataPageV1.getValueCount(),
              dataPageV1.getBytes().toByteArray(),
              definitionLevelReader.getNextOffset());

            validateStatsV1(page, definitionLevelReader, valuesReader);
            return null;
          } catch (IOException ex) { throw new RuntimeException("Failed to read page data."); }
        }

        @Override
        public Object visit(DataPageV2 dataPageV2) {
          try {
            RunLengthBitPackingHybridDecoder definitionLevelReader
              = new RunLengthBitPackingHybridDecoder(1, new ByteArrayInputStream(dataPageV2.getDefinitionLevels().toByteArray()));

            ValuesReader valuesReader = dataPageV2
              .getDataEncoding()
              .getValuesReader(columnDescriptor, ValuesType.VALUES);

            valuesReader.initFromPage(dataPageV2.getValueCount(),
              dataPageV2.getData().toByteArray(), 0);

            validateStatsV2(page, definitionLevelReader, valuesReader);
            return null;
          } catch (IOException ex) { throw new RuntimeException("Failed to read page data."); }
        }

        private void validateStatsV1(DataPage page, ValuesReader definitionLevelReader, ValuesReader valuesReader) {
          Statistics<T> statistics = getStatisticsFromPageHeader(page);

          T current;
          T minimum = statistics.genericGetMin();
          T maximum = statistics.genericGetMax();
          int totalNulls = 0;
          for (int index = 0; index < page.getValueCount(); index++) {
            if (definitionLevelReader.readInteger() != 0) {
              current = readValue(valuesReader);
              assertTrue("min should be <= all values", minimum.compareTo(current) <= 0);
              assertTrue("min should be >= all values", maximum.compareTo(current) >= 0);
            } else {
              totalNulls += 1;
            }
          }

          assertEquals("number of nulls should match", statistics.getNumNulls(), totalNulls);
        }

        private void validateStatsV2(DataPage page, RunLengthBitPackingHybridDecoder definitionLevelReader, ValuesReader valuesReader) throws IOException {
          Statistics<T> statistics = getStatisticsFromPageHeader(page);

          T current;
          T minimum = statistics.genericGetMin();
          T maximum = statistics.genericGetMax();
          int totalNulls = 0;
          for (int index = 0; index < page.getValueCount(); index++) {
            if (definitionLevelReader.readInt() != 0) {
              current = readValue(valuesReader);
              assertTrue("min should be <= all values", minimum.compareTo(current) <= 0);
              assertTrue("min should be >= all values", maximum.compareTo(current) >= 0);
            } else {
              totalNulls += 1;
            }
          }

          assertEquals("number of nulls should match", statistics.getNumNulls(), totalNulls);
        }
      });
    }

    public abstract T readValue(ValuesReader valuesReader);
  }

  public static class DataContext extends DataGenerationContext.WriteContext {
    private static final int MAX_TOTAL_ROWS = 1000000;
    private static final int SIZEOF_BYTE = 8;
    private static final int SIZEOF_INT96 = (SIZEOF_BYTE * 12);

    private final long seed;
    private final Random random;
    private final int recordCount;

    private final RandomValueGenerators.RandomIntGenerator intGenerator = new RandomValueGenerators.RandomIntGenerator(RANDOM_SEED);
    private final RandomValueGenerators.RandomLongGenerator longGenerator = new RandomValueGenerators.RandomLongGenerator(RANDOM_SEED);
    private final RandomValueGenerators.RandomInt96Generator int96Generator = new RandomValueGenerators.RandomInt96Generator(RANDOM_SEED);
    private final RandomValueGenerators.RandomFloatGenerator floatGenerator = new RandomValueGenerators.RandomFloatGenerator(RANDOM_SEED);
    private final RandomValueGenerators.RandomDoubleGenerator doubleGenerator = new RandomValueGenerators.RandomDoubleGenerator(RANDOM_SEED);
    private final RandomValueGenerators.RandomStringGenerator stringGenerator = new RandomValueGenerators.RandomStringGenerator(RANDOM_SEED);
    private final RandomValueGenerators.RandomBinaryGenerator binaryGenerator = new RandomValueGenerators.RandomBinaryGenerator(RANDOM_SEED);
    private final RandomValueGenerators.RandomFixedBinaryGenerator fixedBinaryGenerator = new RandomValueGenerators.RandomFixedBinaryGenerator(RANDOM_SEED, determineFixBinaryLength(super.schema));

    public DataContext(long seed, String path, int blockSize, int pageSize, boolean enableDictionary, ParquetProperties.WriterVersion version) throws IOException {
      super(path, buildSchema(seed), blockSize, pageSize, enableDictionary, true, version);

      this.seed = seed;
      this.random = new Random(seed);
      this.recordCount = random.nextInt(MAX_TOTAL_ROWS);
    }

    private static int determineFixBinaryLength(MessageType schema) {
      Type fixedBinaryField = schema.getType("fixed-binary");
      PrimitiveType fixedBinaryFieldAsPrimitiveType = (PrimitiveType)fixedBinaryField;
      return fixedBinaryFieldAsPrimitiveType.getTypeLength();
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
        new PrimitiveType(OPTIONAL, INT32, "unconstrained-i32"),
        new PrimitiveType(OPTIONAL, INT64, "unconstrained-i64"),
        new PrimitiveType(OPTIONAL, FLOAT, "unconstrained-sngl"),
        new PrimitiveType(OPTIONAL, DOUBLE, "unconstrained-dbl")
      );
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < recordCount; index++) {
        Group group = new SimpleGroup(super.schema);

        Integer intValue = intGenerator.nextValue();
        Long longValue = longGenerator.nextValue();
        Binary int96Value = int96Generator.nextBinaryValue();
        Float floatValue = floatGenerator.nextValue();
        Double doubleValue = doubleGenerator.nextValue();
        Binary stringValue = stringGenerator.nextBinaryValue();
        Binary binaryValue = binaryGenerator.nextBinaryValue();
        Binary fixedBinaryValue = fixedBinaryGenerator.nextBinaryValue();

        if (intValue != null) { group.append("i32", intValue); }
        if (longValue != null) { group.append("i64", longValue); }
        if (int96Value != null) { group.append("i96", int96Value); }
        if (floatValue != null) { group.append("sngl", floatValue); }
        if (doubleValue != null) { group.append("dbl", doubleValue); }
        if (stringValue != null) { group.append("strings", stringValue); }
        if (binaryValue != null) { group.append("binary", binaryValue); }
        if (fixedBinaryValue != null) { group.append("fixed-binary", fixedBinaryValue); }

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
      ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, super.fsPath, ParquetMetadataConverter.NO_FILTER);
      ParquetFileReader reader = new ParquetFileReader(configuration,
        metadata.getFileMetaData(),
        super.fsPath,
        metadata.getBlocks(),
        metadata.getFileMetaData().getSchema().getColumns());

      PagingValidator<Integer> i32validator = new PagingValidator<Integer>(columnDescriptorByPath("i32")) {
        @Override
        public Integer readValue(ValuesReader valuesReader) {
          return valuesReader.readInteger();
        }
      };

      PagingValidator<Long> i64Validator = new PagingValidator<Long>(columnDescriptorByPath("i64")) {
        @Override
        public Long readValue(ValuesReader valuesReader) {
          return valuesReader.readLong();
        }
      };

      PagingValidator<Binary> i96Validator = new PagingValidator<Binary>(columnDescriptorByPath("i96")) {
        @Override
        public Binary readValue(ValuesReader valuesReader) {
          return valuesReader.readBytes();
        }
      };

      PagingValidator<Float> snglValidator = new PagingValidator<Float>(columnDescriptorByPath("sngl")) {
        @Override
        public Float readValue(ValuesReader valuesReader) {
          return valuesReader.readFloat();
        }
      };

      PagingValidator<Double> dblValidator = new PagingValidator<Double>(columnDescriptorByPath("dbl")) {
        @Override
        public Double readValue(ValuesReader valuesReader) {
          return valuesReader.readDouble();
        }
      };

      PagingValidator<Binary> stringValidator = new PagingValidator<Binary>(columnDescriptorByPath("strings")) {
        @Override
        public Binary readValue(ValuesReader valuesReader) {
          return valuesReader.readBytes();
        }
      };

      PagingValidator<Binary> binaryValidator = new PagingValidator<Binary>(columnDescriptorByPath("binary")) {
        @Override
        public Binary readValue(ValuesReader valuesReader) {
          return valuesReader.readBytes();
        }
      };

      PagingValidator<Binary> fixedBinaryValidator = new PagingValidator<Binary>(columnDescriptorByPath("fixed-binary")) {
        @Override
        public Binary readValue(ValuesReader valuesReader) {
          return valuesReader.readBytes();
        }
      };

      PagingValidator<Integer> unconstrainedI32validator = new PagingValidator<Integer>(columnDescriptorByPath("unconstrained-i32")) {
        @Override
        public Integer readValue(ValuesReader valuesReader) {
          return valuesReader.readInteger();
        }
      };

      PagingValidator<Long> unconstrainedI64Validator = new PagingValidator<Long>(columnDescriptorByPath("unconstrained-i64")) {
        @Override
        public Long readValue(ValuesReader valuesReader) {
          return valuesReader.readLong();
        }
      };

      PagingValidator<Float> unconstrainedSnglValidator = new PagingValidator<Float>(columnDescriptorByPath("unconstrained-sngl")) {
        @Override
        public Float readValue(ValuesReader valuesReader) {
          return valuesReader.readFloat();
        }
      };

      PagingValidator<Double> unconstrainedDblValidator = new PagingValidator<Double>(columnDescriptorByPath("unconstrained-dbl")) {
        @Override
        public Double readValue(ValuesReader valuesReader) {
          return valuesReader.readDouble();
        }
      };

      PageReadStore pageReadStore;
      while ((pageReadStore = reader.readNextRowGroup()) != null) {
        i32validator.validate(pageReadStore);
        i64Validator.validate(pageReadStore);
        i96Validator.validate(pageReadStore);
        snglValidator.validate(pageReadStore);
        dblValidator.validate(pageReadStore);
        stringValidator.validate(pageReadStore);
        binaryValidator.validate(pageReadStore);
        fixedBinaryValidator.validate(pageReadStore);

        unconstrainedI32validator.validate(pageReadStore);
        unconstrainedI64Validator.validate(pageReadStore);
        unconstrainedSnglValidator.validate(pageReadStore);
        unconstrainedDblValidator.validate(pageReadStore);
      }
    }

    private ColumnDescriptor columnDescriptorByPath(String path) {
      return schema.getColumnDescription(path.split("\\."));
    }
  }

  public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException{
    folder.create();
  }

  @After
  public void tearDown() throws IOException {
    folder.delete();
  }

  @Test
  public void testStatistics() throws IOException {
    String filename = String.format("%s/test_file%s.parq", folder.getRoot(), RANDOM_SEED);
    String configuration = String.format("FILENAME %s, SEED: %s", filename, RANDOM_SEED);
    System.out.println(configuration);

    Random random = new Random(RANDOM_SEED);

    int blockSize =(random.nextInt(100) + 10) * MEGABYTE;
    int pageSize = (random.nextInt(10) + 1) * MEGABYTE;

    DataContext parquetOneTest = new DataContext(RANDOM_SEED, filename, blockSize, pageSize, false, ParquetProperties.WriterVersion.PARQUET_1_0);
    DataContext parquetTwoTest = new DataContext(RANDOM_SEED, filename, blockSize, pageSize, false, ParquetProperties.WriterVersion.PARQUET_2_0);

    for (DataContext test : new DataContext[] { parquetOneTest, parquetTwoTest }) {
      DataGenerationContext.writeAndTest(test);
    }
  }


}
