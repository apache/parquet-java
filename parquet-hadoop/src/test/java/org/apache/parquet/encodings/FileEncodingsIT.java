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
package org.apache.parquet.encodings;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.parquet.statistics.RandomValues;
import org.apache.parquet.statistics.TestStatistics;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains test cases to validate each data type encoding.
 * Each test runs against all Parquet writer versions.
 * All data types are validating with and without dictionary encoding.
 */
@RunWith(Parameterized.class)
public class FileEncodingsIT {

  private static final Logger LOG = LoggerFactory.getLogger(FileEncodingsIT.class);

  private static final int RANDOM_SEED = 1;
  private static final int RECORD_COUNT = 2000000;
  private static final int FIXED_LENGTH = 60;
  private static final int TEST_PAGE_SIZE = 16 * 1024; // 16K
  private static final int TEST_ROW_GROUP_SIZE = 128 * 1024; // 128K
  private static final int TEST_DICT_PAGE_SIZE = TEST_PAGE_SIZE;

  private static final Configuration configuration = new Configuration();

  private static RandomValues.IntGenerator intGenerator;
  private static RandomValues.LongGenerator longGenerator;
  private static RandomValues.Int96Generator int96Generator;
  private static RandomValues.FloatGenerator floatGenerator;
  private static RandomValues.DoubleGenerator doubleGenerator;
  private static RandomValues.BinaryGenerator binaryGenerator;
  private static RandomValues.FixedGenerator fixedBinaryGenerator;

  // Parameters
  private PrimitiveTypeName paramTypeName;
  private CompressionCodecName compression;
  private TrackingByteBufferAllocator allocator;

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    List<PrimitiveTypeName> types = List.of(
        PrimitiveTypeName.BOOLEAN,
        PrimitiveTypeName.INT32,
        PrimitiveTypeName.INT64,
        PrimitiveTypeName.INT96,
        PrimitiveTypeName.FLOAT,
        PrimitiveTypeName.DOUBLE,
        PrimitiveTypeName.BINARY,
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);

    List<CompressionCodecName> codecs;
    String codecList = System.getenv("TEST_CODECS");
    if (codecList != null) {
      codecs = new ArrayList<CompressionCodecName>();
      for (String codec : codecList.split(",")) {
        codecs.add(CompressionCodecName.valueOf(codec.toUpperCase(Locale.ENGLISH)));
      }
    } else {
      // otherwise test just UNCOMPRESSED
      codecs = List.of(CompressionCodecName.UNCOMPRESSED);
    }

    System.err.println("Testing codecs: " + codecs);

    List<Object[]> parameters = new ArrayList<Object[]>();
    for (PrimitiveTypeName type : types) {
      for (CompressionCodecName codec : codecs) {
        parameters.add(new Object[] {type, codec});
      }
    }

    return parameters;
  }

  public FileEncodingsIT(PrimitiveTypeName typeName, CompressionCodecName compression) {
    this.paramTypeName = typeName;
    this.compression = compression;
  }

  @BeforeClass
  public static void initialize() throws IOException {
    Random random = new Random(RANDOM_SEED);
    intGenerator = new RandomValues.IntGenerator(random.nextLong());
    longGenerator = new RandomValues.LongGenerator(random.nextLong());
    int96Generator = new RandomValues.Int96Generator(random.nextLong());
    floatGenerator = new RandomValues.FloatGenerator(random.nextLong());
    doubleGenerator = new RandomValues.DoubleGenerator(random.nextLong());
    binaryGenerator = new RandomValues.BinaryGenerator(random.nextLong());
    fixedBinaryGenerator = new RandomValues.FixedGenerator(random.nextLong(), FIXED_LENGTH);
  }

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void testFileEncodingsWithoutDictionary() throws Exception {
    final boolean DISABLE_DICTIONARY = false;
    List<?> randomValues;
    randomValues = generateRandomValues(this.paramTypeName, RECORD_COUNT);

    /* Run an encoding test per each writer version.
     * This loop will make sure to test future writer versions added to WriterVersion enum.
     */
    for (WriterVersion writerVersion : WriterVersion.values()) {
      LOG.info(String.format(
          "Testing %s/%s/%s encodings using ROW_GROUP_SIZE=%d PAGE_SIZE=%d",
          writerVersion, this.paramTypeName, this.compression, TEST_ROW_GROUP_SIZE, TEST_PAGE_SIZE));

      Path parquetFile = createTempFile();
      writeValuesToFile(
          parquetFile,
          this.paramTypeName,
          randomValues,
          TEST_ROW_GROUP_SIZE,
          TEST_PAGE_SIZE,
          DISABLE_DICTIONARY,
          writerVersion);
      PageGroupValidator.validatePages(parquetFile, randomValues);
    }
  }

  @Test
  public void testFileEncodingsWithDictionary() throws Exception {
    final boolean ENABLE_DICTIONARY = true;
    List<?> dictionaryValues = generateDictionaryValues(this.paramTypeName, RECORD_COUNT);

    /* Run an encoding test per each writer version.
     * This loop will make sure to test future writer versions added to WriterVersion enum.
     */
    for (WriterVersion writerVersion : WriterVersion.values()) {
      LOG.info(String.format(
          "Testing %s/%s/%s + DICTIONARY encodings using ROW_GROUP_SIZE=%d PAGE_SIZE=%d",
          writerVersion, this.paramTypeName, this.compression, TEST_ROW_GROUP_SIZE, TEST_PAGE_SIZE));

      Path parquetFile = createTempFile();
      writeValuesToFile(
          parquetFile,
          this.paramTypeName,
          dictionaryValues,
          TEST_ROW_GROUP_SIZE,
          TEST_PAGE_SIZE,
          ENABLE_DICTIONARY,
          writerVersion);
      PageGroupValidator.validatePages(parquetFile, dictionaryValues);
    }
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private Path createTempFile() throws IOException {
    File tempFile = tempFolder.newFile();
    tempFile.delete();
    return new Path(tempFile.getAbsolutePath());
  }

  /**
   * Writes a set of values to a parquet file.
   * The ParquetWriter will write the values with dictionary encoding disabled so that we test specific encodings for
   */
  private void writeValuesToFile(
      Path file,
      PrimitiveTypeName type,
      List<?> values,
      int rowGroupSize,
      int pageSize,
      boolean enableDictionary,
      WriterVersion version)
      throws IOException {
    MessageType schema;
    if (type == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
      schema = Types.buildMessage()
          .required(type)
          .length(FIXED_LENGTH)
          .named("field")
          .named("test");
    } else {
      schema = Types.buildMessage().required(type).named("field").named("test");
    }

    SimpleGroupFactory message = new SimpleGroupFactory(schema);
    GroupWriteSupport.setSchema(schema, configuration);

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withAllocator(allocator)
        .withCompressionCodec(compression)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withDictionaryPageSize(TEST_DICT_PAGE_SIZE)
        .withDictionaryEncoding(enableDictionary)
        .withWriterVersion(version)
        .withConf(configuration)
        .build();

    for (Object o : values) {
      switch (type) {
        case BOOLEAN:
          writer.write(message.newGroup().append("field", (Boolean) o));
          break;
        case INT32:
          writer.write(message.newGroup().append("field", (Integer) o));
          break;
        case INT64:
          writer.write(message.newGroup().append("field", (Long) o));
          break;
        case FLOAT:
          writer.write(message.newGroup().append("field", (Float) o));
          break;
        case DOUBLE:
          writer.write(message.newGroup().append("field", (Double) o));
          break;
        case INT96:
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          writer.write(message.newGroup().append("field", (Binary) o));
          break;
        default:
          throw new IllegalArgumentException("Unknown type name: " + type);
      }
    }

    writer.close();
  }

  private List<?> generateRandomValues(PrimitiveTypeName type, int count) {
    List<Object> values = new ArrayList<Object>();

    for (int i = 0; i < count; i++) {
      Object value;
      switch (type) {
        case BOOLEAN:
          value = (intGenerator.nextValue() % 2 == 0) ? true : false;
          break;
        case INT32:
          value = intGenerator.nextValue();
          break;
        case INT64:
          value = longGenerator.nextValue();
          break;
        case FLOAT:
          value = floatGenerator.nextValue();
          break;
        case DOUBLE:
          value = doubleGenerator.nextValue();
          break;
        case INT96:
          value = int96Generator.nextBinaryValue();
          break;
        case BINARY:
          value = binaryGenerator.nextBinaryValue();
          break;
        case FIXED_LEN_BYTE_ARRAY:
          value = fixedBinaryGenerator.nextBinaryValue();
          break;
        default:
          throw new IllegalArgumentException("Unknown type name: " + type);
      }

      values.add(value);
    }

    return values;
  }

  private List<?> generateDictionaryValues(PrimitiveTypeName type, int count) {
    final int DICT_VALUES_SIZE = 100;

    final List<?> DICT_BINARY_VALUES = generateRandomValues(PrimitiveTypeName.BINARY, DICT_VALUES_SIZE);
    final List<?> DICT_INT96_VALUES = generateRandomValues(PrimitiveTypeName.INT96, DICT_VALUES_SIZE);
    final List<?> DICT_FIXED_LEN_VALUES =
        generateRandomValues(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, DICT_VALUES_SIZE);

    List<Object> values = new ArrayList<Object>();

    for (int i = 0; i < count; i++) {
      int dictValue = i % DICT_VALUES_SIZE;
      Object value;
      switch (type) {
        case BOOLEAN:
          value = (i % 2 == 0) ? true : false;
          break;
        case INT32:
          value = dictValue;
          break;
        case INT64:
          value = (long) dictValue;
          break;
        case FLOAT:
          value = (float) dictValue;
          break;
        case DOUBLE:
          value = (double) dictValue;
          break;
        case INT96:
          value = DICT_INT96_VALUES.get(dictValue);
          break;
        case BINARY:
          value = DICT_BINARY_VALUES.get(dictValue);
          break;
        case FIXED_LEN_BYTE_ARRAY:
          value = DICT_FIXED_LEN_VALUES.get(dictValue);
          break;
        default:
          throw new IllegalArgumentException("Unknown type name: " + type);
      }

      values.add(value);
    }

    return values;
  }

  /**
   * This class validates that a group of pages (row group pages) matches the expected values. It checks
   * the values can be read in different order, such as reading first page to last, and reading last page
   * to first.
   */
  private static class PageGroupValidator {
    public static void validatePages(Path file, List<?> expectedValues) throws IOException {
      List<PageReadStore> blockReaders = readBlocksFromFile(file);
      MessageType fileSchema = readSchemaFromFile(file);
      int rowGroupID = 0;
      int rowsRead = 0;
      for (PageReadStore pageReadStore : blockReaders) {
        for (ColumnDescriptor columnsDesc : fileSchema.getColumns()) {
          List<DataPage> pageGroup = getPageGroupForColumn(pageReadStore, columnsDesc);
          DictionaryPage dictPage = reusableCopy(getDictionaryPageForColumn(pageReadStore, columnsDesc));

          List<?> expectedRowGroupValues =
              expectedValues.subList(rowsRead, (int) (rowsRead + pageReadStore.getRowCount()));
          validateFirstToLast(rowGroupID, dictPage, pageGroup, columnsDesc, expectedRowGroupValues);
          validateLastToFirst(rowGroupID, dictPage, pageGroup, columnsDesc, expectedRowGroupValues);
        }

        rowsRead += pageReadStore.getRowCount();
        rowGroupID++;
      }
    }

    private static DictionaryPage reusableCopy(DictionaryPage dict) {
      if (dict == null) {
        return null;
      }
      try {
        return new DictionaryPage(
            BytesInput.from(dict.getBytes().toByteArray()), dict.getDictionarySize(), dict.getEncoding());
      } catch (IOException e) {
        throw new ParquetDecodingException("Cannot read dictionary", e);
      }
    }

    private static DataPage reusableCopy(DataPage page) {
      return page.accept(new DataPage.Visitor<DataPage>() {
        @Override
        public DataPage visit(DataPageV1 data) {
          try {
            return new DataPageV1(
                BytesInput.from(data.getBytes().toByteArray()),
                data.getValueCount(),
                data.getUncompressedSize(),
                data.getStatistics(),
                data.getRlEncoding(),
                data.getDlEncoding(),
                data.getValueEncoding());
          } catch (IOException e) {
            throw new ParquetDecodingException("Cannot read data", e);
          }
        }

        @Override
        public DataPage visit(DataPageV2 data) {
          try {
            return new DataPageV2(
                data.getRowCount(),
                data.getNullCount(),
                data.getValueCount(),
                BytesInput.from(data.getRepetitionLevels().toByteArray()),
                BytesInput.from(data.getDefinitionLevels().toByteArray()),
                data.getDataEncoding(),
                BytesInput.from(data.getData().toByteArray()),
                data.getUncompressedSize(),
                data.getStatistics(),
                data.isCompressed());
          } catch (IOException e) {
            throw new ParquetDecodingException("Cannot read data", e);
          }
        }
      });
    }

    private static void validateFirstToLast(
        int rowGroupID,
        DictionaryPage dictPage,
        List<DataPage> pageGroup,
        ColumnDescriptor desc,
        List<?> expectedValues) {
      int rowsRead = 0, pageID = 0;
      for (DataPage page : pageGroup) {
        List<?> expectedPageValues = expectedValues.subList(rowsRead, rowsRead + page.getValueCount());
        PageValuesValidator.validateValuesForPage(rowGroupID, pageID, dictPage, page, desc, expectedPageValues);
        rowsRead += page.getValueCount();
        pageID++;
      }
    }

    private static void validateLastToFirst(
        int rowGroupID,
        DictionaryPage dictPage,
        List<DataPage> pageGroup,
        ColumnDescriptor desc,
        List<?> expectedValues) {
      int rowsLeft = expectedValues.size();
      for (int pageID = pageGroup.size() - 1; pageID >= 0; pageID--) {
        DataPage page = pageGroup.get(pageID);
        int offset = rowsLeft - page.getValueCount();
        List<?> expectedPageValues = expectedValues.subList(offset, offset + page.getValueCount());
        PageValuesValidator.validateValuesForPage(rowGroupID, pageID, dictPage, page, desc, expectedPageValues);
        rowsLeft -= page.getValueCount();
      }
    }

    private static DictionaryPage getDictionaryPageForColumn(
        PageReadStore pageReadStore, ColumnDescriptor columnDescriptor) {
      PageReader pageReader = pageReadStore.getPageReader(columnDescriptor);
      return pageReader.readDictionaryPage();
    }

    private static List<DataPage> getPageGroupForColumn(
        PageReadStore pageReadStore, ColumnDescriptor columnDescriptor) {
      PageReader pageReader = pageReadStore.getPageReader(columnDescriptor);
      List<DataPage> pageGroup = new ArrayList<DataPage>();

      DataPage page;
      while ((page = pageReader.readPage()) != null) {
        pageGroup.add(reusableCopy(page));
      }

      return pageGroup;
    }

    private static MessageType readSchemaFromFile(Path file) throws IOException {
      ParquetMetadata metadata =
          ParquetFileReader.readFooter(configuration, file, ParquetMetadataConverter.NO_FILTER);
      return metadata.getFileMetaData().getSchema();
    }

    private static List<PageReadStore> readBlocksFromFile(Path file) throws IOException {
      List<PageReadStore> rowGroups = new ArrayList<PageReadStore>();

      ParquetMetadata metadata =
          ParquetFileReader.readFooter(configuration, file, ParquetMetadataConverter.NO_FILTER);
      ParquetFileReader fileReader = new ParquetFileReader(
          configuration,
          metadata.getFileMetaData(),
          file,
          metadata.getBlocks(),
          metadata.getFileMetaData().getSchema().getColumns());

      PageReadStore group;
      while ((group = fileReader.readNextRowGroup()) != null) {
        rowGroups.add(group);
      }

      return rowGroups;
    }
  }

  /**
   * This class is used to validate all values read from a page against a list
   * of expected values.
   */
  private static class PageValuesValidator {
    private List<?> expectedValues;
    private int currentPos;
    private int pageID;
    private int rowGroupID;

    public PageValuesValidator(int rowGroupID, int pageID, List<?> expectedValues) {
      this.rowGroupID = rowGroupID;
      this.pageID = pageID;
      this.expectedValues = expectedValues;
    }

    public void validateNextValue(Object value) {
      assertEquals(
          String.format(
              "Value from page is different than expected, ROW_GROUP_ID=%d PAGE_ID=%d VALUE_POS=%d",
              rowGroupID, pageID, currentPos),
          expectedValues.get(currentPos++),
          value);
    }

    public static void validateValuesForPage(
        int rowGroupID,
        int pageID,
        DictionaryPage dictPage,
        DataPage page,
        ColumnDescriptor columnDesc,
        List<?> expectedValues) {
      TestStatistics.SingletonPageReader pageReader = new TestStatistics.SingletonPageReader(dictPage, page);
      PrimitiveConverter converter = getConverter(rowGroupID, pageID, columnDesc.getType(), expectedValues);
      ColumnReaderImpl column = new ColumnReaderImpl(columnDesc, pageReader, converter, null);
      for (int i = 0; i < pageReader.getTotalValueCount(); i += 1) {
        column.writeCurrentValueToConverter();
        column.consume();
      }
    }

    private static PrimitiveConverter getConverter(
        final int rowGroupID, final int pageID, PrimitiveTypeName type, final List<?> expectedValues) {
      return type.convert(new PrimitiveType.PrimitiveTypeNameConverter<PrimitiveConverter, RuntimeException>() {

        @Override
        public PrimitiveConverter convertFLOAT(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
          final PageValuesValidator validator = new PageValuesValidator(rowGroupID, pageID, expectedValues);
          return new PrimitiveConverter() {
            @Override
            public void addFloat(float value) {
              validator.validateNextValue(value);
            }
          };
        }

        @Override
        public PrimitiveConverter convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
          final PageValuesValidator validator = new PageValuesValidator(rowGroupID, pageID, expectedValues);
          return new PrimitiveConverter() {
            @Override
            public void addDouble(double value) {
              validator.validateNextValue(value);
            }
          };
        }

        @Override
        public PrimitiveConverter convertINT32(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
          final PageValuesValidator validator = new PageValuesValidator(rowGroupID, pageID, expectedValues);
          return new PrimitiveConverter() {
            @Override
            public void addInt(int value) {
              validator.validateNextValue(value);
            }
          };
        }

        @Override
        public PrimitiveConverter convertINT64(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
          final PageValuesValidator validator = new PageValuesValidator(rowGroupID, pageID, expectedValues);
          return new PrimitiveConverter() {
            @Override
            public void addLong(long value) {
              validator.validateNextValue(value);
            }
          };
        }

        @Override
        public PrimitiveConverter convertINT96(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
          return convertBINARY(primitiveTypeName);
        }

        @Override
        public PrimitiveConverter convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName)
            throws RuntimeException {
          return convertBINARY(primitiveTypeName);
        }

        @Override
        public PrimitiveConverter convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
          final PageValuesValidator validator = new PageValuesValidator(rowGroupID, pageID, expectedValues);
          return new PrimitiveConverter() {
            @Override
            public void addBoolean(boolean value) {
              validator.validateNextValue(value);
            }
          };
        }

        @Override
        public PrimitiveConverter convertBINARY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
          final PageValuesValidator validator = new PageValuesValidator(rowGroupID, pageID, expectedValues);
          return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
              validator.validateNextValue(value);
            }
          };
        }
      });
    }
  }
}
