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

import java.io.File;
import java.io.IOException;

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

public class TestReadIntTestFile {
  private static final Log LOG = Log.getLog(TestReadIntTestFile.class);

//  @Test
  public void readTest() throws IOException {
    Path testFile = new Path(new File("testdata/tpch/customer").toURI());
    Configuration configuration = new Configuration(true);
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, testFile);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    ParquetFileReader parquetFileReader = new ParquetFileReader(configuration, testFile, readFooter.getBlocks(), schema.getColumns());
    PageReadStore pages = parquetFileReader.readColumns();
    final long rows = pages.getRowCount();
    LOG.info("rows: "+rows);
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
    for (int i = 0; i < rows; i++) {
      final Group g = recordReader.read();
      LOG.info(i + ": " + g);
    }
  }
}
