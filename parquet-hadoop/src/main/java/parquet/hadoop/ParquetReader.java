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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.schema.MessageType;

/**
 * Read records from a Parquet file.
 */
public class ParquetReader<T> implements Closeable {

  private ParquetRecordReader<T> reader;

  public ParquetReader(Path file, ReadSupport<T> readSupport) throws IOException {
    this(file, readSupport, null);
  }

  public ParquetReader(Path file, ReadSupport<T> readSupport, UnboundRecordFilter filter) throws IOException {
    Configuration conf = new Configuration();

    FileSystem fs = FileSystem.get(conf);
    List<FileStatus> statuses = Arrays.asList(fs.listStatus(file));
    List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses);
    Footer footer = footers.get(0); // TODO: check only one

    List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();
    FileMetaData fileMetaData = footer.getParquetMetadata().getFileMetaData();
    // TODO: this assumes all files have the same schema
    MessageType schema = fileMetaData.getSchema();
    Map<String, String> extraMetadata = fileMetaData.getKeyValueMetaData();
    final ReadContext readContext = readSupport.init(conf, extraMetadata, schema);
    reader = new ParquetRecordReader<T>(readSupport, filter);
    ParquetInputSplit inputSplit =
        new ParquetInputSplit(
            file, 0, 0, null, blocks,
            readContext.getRequestedSchema().toString(),
            schema.toString(),
            extraMetadata,
            readContext.getReadSupportMetadata());
    reader.initialize(inputSplit, conf);
  }

  public T read() throws IOException {
    try {
      return reader.nextKeyValue() ? reader.getCurrentValue() : null;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
