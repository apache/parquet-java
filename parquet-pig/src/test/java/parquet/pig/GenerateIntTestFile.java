/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.pig;

import static parquet.column.Encoding.PLAIN;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.mem.MemColumnWriteStore;
import parquet.column.mem.MemPageStore;
import parquet.column.mem.Page;
import parquet.column.mem.PageReadStore;
import parquet.column.mem.PageReader;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

public class GenerateIntTestFile {
  private static final Log LOG = Log.getLog(GenerateIntTestFile.class);

  public static void main(String[] args) throws Throwable {
    File out = new File("testdata/from_java/int_test_file");
    if (out.exists()) {
      if (!out.delete()) {
        throw new RuntimeException("can not remove existing file " + out.getAbsolutePath());
      }
    }
    Path testFile = new Path(out.toURI());
    Configuration configuration = new Configuration();
    {
      MessageType schema = new MessageType("int_test_file", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "int_col"));

      MemPageStore pageStore = new MemPageStore();
      MemColumnWriteStore store = new MemColumnWriteStore(pageStore, 8*1024);
      //
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

      RecordConsumer recordWriter = columnIO.getRecordWriter(store);

      int recordCount = 0;
      for (int i = 0; i < 100; i++) {
        recordWriter.startMessage();
        recordWriter.startField("int_col", 0);
        if (i % 10 != 0) {
          recordWriter.addInteger(i);
        }
        recordWriter.endField("int_col", 0);
        recordWriter.endMessage();
        ++ recordCount;
      }
      store.flush();


      writeToFile(testFile, configuration, schema, pageStore, recordCount);
    }

    {
      readTestFile(testFile, configuration);
    }
  }

  public static void readTestFile(Path testFile, Configuration configuration)
      throws IOException {
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, testFile);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    ParquetFileReader parquetFileReader = new ParquetFileReader(configuration, testFile, readFooter.getBlocks(), schema.getColumns());
    PageReadStore pages = parquetFileReader.readNextRowGroup();
    System.out.println(pages.getRowCount());
  }

  public static void writeToFile(Path file, Configuration configuration, MessageType schema, MemPageStore pageStore, int recordCount)
      throws IOException {
    ParquetFileWriter w = startFile(file, configuration, schema);
    writeBlock(schema, pageStore, recordCount, w);
    endFile(w);
  }

  public static void endFile(ParquetFileWriter w) throws IOException {
    w.end(new HashMap<String, String>());
  }

  public static void writeBlock(MessageType schema, MemPageStore pageStore,
      int recordCount, ParquetFileWriter w) throws IOException {
    w.startBlock(recordCount);
    List<ColumnDescriptor> columns = schema.getColumns();
    for (ColumnDescriptor columnDescriptor : columns) {
      PageReader pageReader = pageStore.getPageReader(columnDescriptor);
      long totalValueCount = pageReader.getTotalValueCount();
      w.startColumn(columnDescriptor, totalValueCount, CompressionCodecName.UNCOMPRESSED);
      int n = 0;
      do {
        Page page = pageReader.readPage();
        n += page.getValueCount();
        // TODO: change INTFC
        w.writeDataPage(page.getValueCount(), (int)page.getBytes().size(), BytesInput.from(page.getBytes().toByteArray()), PLAIN);
      } while (n < totalValueCount);
      w.endColumn();
    }
    w.endBlock();
  }

  public static ParquetFileWriter startFile(Path file,
      Configuration configuration, MessageType schema) throws IOException {
    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, file);
    w.start();
    return w;
  }
}
