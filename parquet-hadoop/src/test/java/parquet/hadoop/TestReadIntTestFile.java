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
package parquet.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.Log;
import parquet.column.mem.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.GroupRecordConsumer;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.schema.MessageType;
import parquet.schema.Type;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class TestReadIntTestFile {
  private static final Log LOG = Log.getLog(TestReadIntTestFile.class);

//  @Test // TODO move this test to a specific compatibility test repo
  public void readTest() throws IOException {

    File baseDir = new File("/Users/julien/github/Parquet/parquet-format/testdata/tpch");
    final File[] parquetFiles = baseDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".parquet");
      }
    });

    for (File parquetFile : parquetFiles) {
      convertToCSV(parquetFile);
    }
  }

  private void convertToCSV(File parquetFile) throws IOException {
    LOG.info("converting " + parquetFile.getName());
    Path testInputFile = new Path(parquetFile.toURI());
    File expectedOutputFile = new File(
        parquetFile.getParentFile(),
        parquetFile.getName().substring(0, parquetFile.getName().length() - ".parquet".length()) + ".csv");
    File csvOutputFile = new File("target/test/fromExampleFiles", parquetFile.getName()+".readFromJava.csv");
    csvOutputFile.getParentFile().mkdirs();
    Configuration configuration = new Configuration(true);
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, testInputFile);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    ParquetFileReader parquetFileReader = new ParquetFileReader(configuration, testInputFile, readFooter.getBlocks(), schema.getColumns());
    PageReadStore pages = parquetFileReader.readColumns();
    final long rows = pages.getRowCount();
    LOG.info("rows: "+rows);
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    try {
      for (int i = 0; i < rows; i++) {
        final Group g = recordReader.read();
        for (int j = 0; j < schema.getFieldCount(); j++) {
          final Type type = schema.getFields().get(j);
          if (j > 0) {
            w.write('|');
//            System.out.print("|");
          }
          String valueToString = g.getValueToString(j, 0);
          if (type.isPrimitive()
              && (type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.FLOAT
                || type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE)
              && valueToString.endsWith(".0")) {
            valueToString = valueToString.substring(0, valueToString.length() - 2);
          }
          w.write(valueToString);// no repetition here
        }
        w.write('\n');
        //      LOG.info(i + ": " + g);
      }
    } finally {
      w.close();
    }
    verify(expectedOutputFile, csvOutputFile);
    LOG.info("verified " + parquetFile.getName());
  }

  private void verify(File expectedOutputFile, File csvOutputFile) throws IOException {
    final BufferedReader expected = new BufferedReader(new FileReader(expectedOutputFile));
    final BufferedReader out = new BufferedReader(new FileReader(csvOutputFile));
    String lineIn;
    String lineOut = null;
    int lineNumber = 0;
    while ((lineIn = expected.readLine()) != null && (lineOut = out.readLine()) != null) {
      ++ lineNumber;
      lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
      assertEquals("line " + lineNumber, lineIn, lineOut);
    }
    assertNull("line " + lineNumber, lineIn);
    assertNull("line " + lineNumber, out.readLine());
    expected.close();
    out.close();
  }
}
