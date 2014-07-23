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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.filter2.compat.FilterCompat;
import parquet.filter2.compat.FilterCompat.Filter;
import parquet.filter2.compat.RowGroupFilter;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.GlobalMetaData;
import parquet.schema.MessageType;

import static parquet.Preconditions.checkNotNull;

/**
 * Read records from a Parquet file.
 * // TODO(alexlevenson): too many constructors
 */
public class ParquetReader<T> implements Closeable {

  private ReadSupport<T> readSupport;
  private Configuration conf;
  private ReadContext readContext;
  private Iterator<Footer> footersIterator;
  private InternalParquetRecordReader<T> reader;
  private GlobalMetaData globalMetaData;
  private final Filter filter;

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   */
  public ParquetReader(Path file, ReadSupport<T> readSupport) throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   */
  public ParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport) throws IOException {
    this(conf, file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @param filter the filter to use to filter records
   * @throws IOException
   */
  public ParquetReader(Path file, ReadSupport<T> readSupport, Filter filter) throws IOException {
    this(new Configuration(), file, readSupport, filter);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @param filter the filter to use to filter records
   * @throws IOException
   */
  public ParquetReader(Configuration conf,
                       Path file,
                       ReadSupport<T> readSupport,
                       Filter filter) throws IOException {
    this.readSupport = readSupport;

    this.filter = checkNotNull(filter, "filter");

    this.conf = conf;

    FileSystem fs = file.getFileSystem(conf);
    List<FileStatus> statuses = Arrays.asList(fs.listStatus(file));
    List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses);
    this.footersIterator = footers.iterator();
    globalMetaData = ParquetFileWriter.getGlobalMetaData(footers);
    MessageType schema = globalMetaData.getSchema();
    Map<String, Set<String>> extraMetadata = globalMetaData.getKeyValueMetaData();
    readContext = readSupport.init(new InitContext(conf, extraMetadata, schema));
  }

  /**
   * @return the next record or null if finished
   * @throws IOException
   */
  public T read() throws IOException {
    try {
      if (reader != null && reader.nextKeyValue()) {
        return reader.getCurrentValue();
      } else {
        initReader();
        return reader == null ? null : read();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void initReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (footersIterator.hasNext()) {
      Footer footer = footersIterator.next();

      List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();

      List<BlockMetaData> filteredBlocks = RowGroupFilter.filterRowGroups(filter, blocks, footer.getParquetMetadata().getFileMetaData().getSchema());

      reader = new InternalParquetRecordReader<T>(readSupport, filter);
      reader.initialize(
          readContext.getRequestedSchema(), globalMetaData.getSchema(), footer.getParquetMetadata().getFileMetaData().getKeyValueMetaData(),
          readContext.getReadSupportMetadata(), footer.getFile(), filteredBlocks, conf);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }
}
