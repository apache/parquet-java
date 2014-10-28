package parquet.column.impl;

import static junit.framework.Assert.assertEquals;
import static parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

import java.util.List;

import org.junit.Test;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.ParquetProperties;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV2;
import parquet.column.page.mem.MemPageReader;
import parquet.column.page.mem.MemPageWriter;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class TestColumnReaderImpl {

  private int rows = 13001;

  private static final class ValidatingConverter extends PrimitiveConverter {
    int count;

    @Override
    public void addBinary(Binary value) {
      assertEquals("bar" + count % 10, value.toStringUsingUTF8());
      ++ count;
    }
  }

  @Test
  public void test() {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary foo; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 columnWriterV2 = new ColumnWriterV2(col, pageWriter, 1024, new ParquetProperties(1024, PARQUET_2_0, true));
    for (int i = 0; i < rows; i++) {
      columnWriterV2.write(Binary.fromString("bar" + i % 10), 0, 0);
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage(i);
      }
    }
    columnWriterV2.writePage(rows);
    columnWriterV2.finalizeColumnChunk();
    List<DataPage> pages = pageWriter.getPages();
    int valueCount = 0;
    int rowCount = 0;
    for (DataPage dataPage : pages) {
      valueCount += dataPage.getValueCount();
      rowCount += ((DataPageV2)dataPage).getRowCount();
    }
    assertEquals(rows, rowCount);
    assertEquals(rows, valueCount);
    MemPageReader pageReader = new MemPageReader((long)rows, pages.iterator(), pageWriter.getDictionaryPage());
    ValidatingConverter converter = new ValidatingConverter();
    ColumnReader columnReader = new ColumnReaderImpl(col, pageReader, converter);
    for (int i = 0; i < rows; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      assertEquals(0, columnReader.getCurrentDefinitionLevel());
      columnReader.writeCurrentValueToConverter();
      columnReader.consume();
    }
    assertEquals(rows, converter.count);
  }

  @Test
  public void testOptional() {
    MessageType schema = MessageTypeParser.parseMessageType("message test { optional binary foo; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 columnWriterV2 = new ColumnWriterV2(col, pageWriter, 1024, new ParquetProperties(1024, PARQUET_2_0, true));
    for (int i = 0; i < rows; i++) {
      columnWriterV2.writeNull(0, 0);
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage(i);
      }
    }
    columnWriterV2.writePage(rows);
    columnWriterV2.finalizeColumnChunk();
    List<DataPage> pages = pageWriter.getPages();
    int valueCount = 0;
    int rowCount = 0;
    for (DataPage dataPage : pages) {
      valueCount += dataPage.getValueCount();
      rowCount += ((DataPageV2)dataPage).getRowCount();
    }
    assertEquals(rows, rowCount);
    assertEquals(rows, valueCount);
    MemPageReader pageReader = new MemPageReader((long)rows, pages.iterator(), pageWriter.getDictionaryPage());
    ValidatingConverter converter = new ValidatingConverter();
    ColumnReader columnReader = new ColumnReaderImpl(col, pageReader, converter);
    for (int i = 0; i < rows; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      assertEquals(0, columnReader.getCurrentDefinitionLevel());
      columnReader.consume();
    }
    assertEquals(0, converter.count);
  }

}
