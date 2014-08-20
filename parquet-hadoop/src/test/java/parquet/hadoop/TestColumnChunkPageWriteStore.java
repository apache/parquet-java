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
package parquet.hadoop;

import static org.junit.Assert.assertEquals;
import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static parquet.hadoop.metadata.CompressionCodecName.GZIP;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.bytes.HeapByteBufferAllocator;
import parquet.bytes.LittleEndianDataInputStream;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.page.DataPageV2;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.column.page.PageWriter;
import parquet.column.statistics.BinaryStatistics;
import parquet.column.statistics.Statistics;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class TestColumnChunkPageWriteStore {

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path file = new Path("target/test/TestColumnChunkPageWriteStore/test.parquet");
    Path root = file.getParent();
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(root)) {
      fs.delete(root, true);
    }
    fs.mkdirs(root);
    CodecFactory f = new CodecFactory(conf);
    int pageSize = 1024;
    int initialSize = 1024;
    MessageType schema = MessageTypeParser.parseMessageType("message test { repeated binary bar; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    Encoding dataEncoding = Encoding.PLAIN;
    int valueCount = 10;
    int d = 1;
    int r = 2;
    int v = 3;
    BytesInput definitionLevels = BytesInput.fromInt(d);
    BytesInput repetitionLevels = BytesInput.fromInt(r);
    Statistics<?> statistics = new BinaryStatistics();
    BytesInput data = BytesInput.fromInt(v);
    int rowCount = 5;
    int nullCount = 1;
    CompressionCodecName codec = GZIP;

    {
      ParquetFileWriter writer = new ParquetFileWriter(conf, schema, file);
      writer.start();
      writer.startBlock(rowCount);
      {
        ColumnChunkPageWriteStore store = new ColumnChunkPageWriteStore(f.getCompressor(codec, pageSize ), schema , initialSize, new HeapByteBufferAllocator());
        PageWriter pageWriter = store.getPageWriter(col);
        pageWriter.writePageV2(
            rowCount, nullCount, valueCount,
            repetitionLevels, definitionLevels,
            dataEncoding, data,
            statistics);
        store.flushToFileWriter(writer);
      }
      writer.endBlock();
      writer.end(new HashMap<String, String>());
    }

    {
      ParquetMetadata footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
      ParquetFileReader reader = new ParquetFileReader(conf, file, footer.getBlocks(), schema.getColumns());
      PageReadStore rowGroup = reader.readNextRowGroup();
      PageReader pageReader = rowGroup.getPageReader(col);
      DataPageV2 page = (DataPageV2)pageReader.readPage();
      assertEquals(rowCount, page.getRowCount());
      assertEquals(nullCount, page.getNullCount());
      assertEquals(valueCount, page.getValueCount());
      assertEquals(d, intValue(page.getDefinitionLevels()));
      assertEquals(r, intValue(page.getRepetitionLevels()));
      assertEquals(dataEncoding, page.getDataEncoding());
      assertEquals(v, intValue(page.getData()));
      assertEquals(statistics.toString(), page.getStatistics().toString());
      reader.close();
    }
  }

  private int intValue(BytesInput in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    in.writeAllTo(baos);
    LittleEndianDataInputStream os = new LittleEndianDataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    int i = os.readInt();
    os.close();
    return i;
  }

}
