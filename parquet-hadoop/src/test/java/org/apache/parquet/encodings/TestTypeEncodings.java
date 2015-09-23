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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.statistics.RandomValues;
import org.apache.parquet.statistics.TestStatistics;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

@RunWith(Parameterized.class)
public class TestTypeEncodings {
  private static final int RANDOM_SEED = 1;
  private static final int RECORD_COUNT = 1000000;
  private static final int FIXED_LENGTH = 60;

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

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    return Arrays.asList(new Object[][] {
        { PrimitiveTypeName.BOOLEAN },
        { PrimitiveTypeName.INT32 },
        { PrimitiveTypeName.INT64 },
        { PrimitiveTypeName.INT96 },
        { PrimitiveTypeName.FLOAT },
        { PrimitiveTypeName.DOUBLE },
        { PrimitiveTypeName.BINARY },
        { PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY }
    });
  }

  public TestTypeEncodings(PrimitiveTypeName typeName) {
    this.paramTypeName = typeName;
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

  @Test
  public void testTypeEncodings() throws Exception {
    int pageSize = 64; // 64B
    int rowGroupSize;

    // Let's use a smaller row group size for booleans so that it span
    // many pages.
    if (this.paramTypeName == PrimitiveTypeName.BOOLEAN) {
      rowGroupSize = 32 * 1024;  // 32K
    } else {
      rowGroupSize = 256 * 1024; // 256K
    }

    Path parquet1 = createTempFile();
    Path parquet2 = createTempFile();

    System.out.println(String.format("Testing %s encodings using ROW_GROUP_SIZE=%d PAGE_SIZE=%d",
        this.paramTypeName.toString(), rowGroupSize, pageSize));

    List<?> randomValues = generateRandomValues(this.paramTypeName, RECORD_COUNT);

    writeValuesToFile(parquet1, this.paramTypeName, randomValues, rowGroupSize, pageSize, PARQUET_1_0);
    writeValuesToFile(parquet2, this.paramTypeName, randomValues, rowGroupSize, pageSize, PARQUET_2_0);

    PageGroupValidator.validatePages(parquet1, randomValues);
    PageGroupValidator.validatePages(parquet2, randomValues);
  }

  private Path createTempFile() throws IOException {
    TemporaryFolder tempFolder = new TemporaryFolder();
    File file = tempFolder.newFile();
    file.delete();
    file.deleteOnExit();
    return new Path(file.getAbsolutePath());
  }

  private void writeValuesToFile(Path file, PrimitiveTypeName type, List<?> values, int rowGroupSize, int pageSize, WriterVersion version) throws IOException {
    MessageType schema;
    if (type == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
      schema = Types.buildMessage().required(type).length(FIXED_LENGTH).named("field").named("test");
    } else {
      schema = Types.buildMessage().required(type).named("field").named("test");
    }

    SimpleGroupFactory message = new SimpleGroupFactory(schema);
    GroupWriteSupport.setSchema(schema, configuration);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(file, new GroupWriteSupport(),
        CompressionCodecName.UNCOMPRESSED, rowGroupSize, pageSize, 0, false, false, version, configuration);

    for (Object o: values) {
      switch (type) {
        case BOOLEAN:
          writer.write(message.newGroup().append("field", (Boolean)o));
        break;
        case INT32:
          writer.write(message.newGroup().append("field", (Integer)o));
        break;
        case INT64:
          writer.write(message.newGroup().append("field", (Long)o));
        break;
        case FLOAT:
          writer.write(message.newGroup().append("field", (Float)o));
        break;
        case DOUBLE:
          writer.write(message.newGroup().append("field", (Double)o));
        break;
        case INT96:
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          writer.write(message.newGroup().append("field", (Binary)o));
        break;
        default:
          throw new IllegalArgumentException("Unknown type name: " + type);
      }
    }

    writer.close();
  }

  private List<?> generateRandomValues(PrimitiveTypeName type, int count) {
    List<Object> values = new ArrayList<Object>();

    for (int i=0; i<count; i++) {
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

          List<?> expectedRowGroupValues = expectedValues.subList(rowsRead, (int)(rowsRead + pageReadStore.getRowCount()));
          validateFirstToLast(rowGroupID, pageGroup, columnsDesc, expectedRowGroupValues);
          validateLastToFirst(rowGroupID, pageGroup, columnsDesc, expectedRowGroupValues);
        }

        rowsRead += pageReadStore.getRowCount();
        rowGroupID++;
      }
    }

    private static void validateFirstToLast(int rowGroupID, List<DataPage> pageGroup, ColumnDescriptor desc, List<?> expectedValues) {
      int rowsRead = 0, pageID = 0;
      for (DataPage page : pageGroup) {
        List<?> expectedPageValues = expectedValues.subList(rowsRead, rowsRead + page.getValueCount());
        PageValuesValidator.validateValuesForPage(rowGroupID, pageID, page, desc, expectedPageValues);
        rowsRead += page.getValueCount();
        pageID++;
      }
    }

    private static void validateLastToFirst(int rowGroupID, List<DataPage> pageGroup, ColumnDescriptor desc, List<?> expectedValues) {
      int rowsLeft = expectedValues.size();
      for (int pageID = pageGroup.size() - 1; pageID >= 0; pageID--) {
        DataPage page = pageGroup.get(pageID);
        int offset = rowsLeft - page.getValueCount();
        List<?> expectedPageValues = expectedValues.subList(offset, offset + page.getValueCount());
        PageValuesValidator.validateValuesForPage(rowGroupID, pageID, page, desc, expectedPageValues);
        rowsLeft -= page.getValueCount();
      }
    }

    private static List<DataPage> getPageGroupForColumn(PageReadStore pageReadStore, ColumnDescriptor columnDescriptor) {
      PageReader pageReader = pageReadStore.getPageReader(columnDescriptor);
      List<DataPage> pageGroup = new ArrayList<DataPage>();

      DataPage page;
      while ((page = pageReader.readPage()) != null) {
        pageGroup.add(page);
      }

      return pageGroup;
    }

    private static MessageType readSchemaFromFile(Path file) throws IOException {
      ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, file, ParquetMetadataConverter.NO_FILTER);
      return metadata.getFileMetaData().getSchema();
    }


    private static List<PageReadStore> readBlocksFromFile(Path file) throws IOException {
      List<PageReadStore> rowGroups = new ArrayList<PageReadStore>();

      ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, file, ParquetMetadataConverter.NO_FILTER);
      ParquetFileReader fileReader = new ParquetFileReader(configuration, metadata.getFileMetaData(), file, metadata.getBlocks(),
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
      assertEquals(String.format("Value from page is different than expected, ROW_GROUP_ID=%d PAGE_ID=%d VALUE_POS=%d",
          rowGroupID, pageID, currentPos), expectedValues.get(currentPos++), value);
    }

    public static void validateValuesForPage(int rowGroupID, int pageID, DataPage page, ColumnDescriptor columnDesc, List<?> expectedValues) {
      TestStatistics.SingletonPageReader pageReader = new TestStatistics.SingletonPageReader(null, page);
      PrimitiveConverter converter = getConverter(rowGroupID, pageID, columnDesc.getType(), expectedValues);
      ColumnReaderImpl column = new ColumnReaderImpl(columnDesc, pageReader, converter, null);
      for (int i = 0; i < pageReader.getTotalValueCount(); i += 1) {
        column.writeCurrentValueToConverter();
        column.consume();
      }
    }

    private static PrimitiveConverter getConverter(final int rowGroupID, final int pageID, PrimitiveTypeName type, final List<?> expectedValues) {
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
        public PrimitiveConverter convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
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
