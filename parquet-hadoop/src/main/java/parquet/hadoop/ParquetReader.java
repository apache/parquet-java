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

import static parquet.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.PathFilter;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.compat.FilterCompat.Filter;
import parquet.filter2.compat.RowGroupFilter;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.schema.MessageType;

/**
 * Read records from a Parquet file.
 * TODO: too many constructors (https://issues.apache.org/jira/browse/PARQUET-39)
 */
public class ParquetReader<T> implements Closeable {

  private final ReadSupport<T> readSupport;
  private final Configuration conf;
  private final Iterator<Footer> footersIterator;
  private final Filter filter;

  private InternalParquetRecordReader<T> reader;

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Path file, ReadSupport<T> readSupport) throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport) throws IOException {
    this(conf, file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @param unboundRecordFilter the filter to use to filter records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter) throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @param unboundRecordFilter the filter to use to filter records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter) throws IOException {
    this(conf, file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  private ParquetReader(Configuration conf,
                       Path file,
                       ReadSupport<T> readSupport,
                       Filter filter) throws IOException {
    this.readSupport = readSupport;
    this.filter = checkNotNull(filter, "filter");
    this.conf = conf;

    FileSystem fs = file.getFileSystem(conf);
    List<FileStatus> statuses = Arrays.asList(fs.listStatus(file, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        return !p.getName().startsWith("_") && !p.getName().startsWith(".");
      }
    }));
    List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
    this.footersIterator = footers.iterator();
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

      MessageType fileSchema = footer.getParquetMetadata().getFileMetaData().getSchema();

      List<BlockMetaData> filteredBlocks = RowGroupFilter.filterRowGroups(
          filter, blocks, fileSchema);

      reader = new InternalParquetRecordReader<T>(readSupport, filter);
      reader.initialize(fileSchema,
          footer.getParquetMetadata().getFileMetaData().getKeyValueMetaData(),
          footer.getFile(), filteredBlocks, conf);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  public static <T> Builder<T> builder(ReadSupport<T> readSupport, Path path) {
    return new Builder<T>(readSupport, path);
  }

  public static class Builder<T> {
    private final ReadSupport<T> readSupport;
    private final Path file;
    private Configuration conf;
    private Filter filter;

    private Builder(ReadSupport<T> readSupport, Path path) {
      this.readSupport = checkNotNull(readSupport, "readSupport");
      this.file = checkNotNull(path, "path");
      this.conf = new Configuration();
      this.filter = FilterCompat.NOOP;
    }

    public Builder<T> withConf(Configuration conf) {
      this.conf = checkNotNull(conf, "conf");
      return this;
    }

    public Builder<T> withFilter(Filter filter) {
      this.filter = checkNotNull(filter, "filter");
      return this;
    }

    public ParquetReader<T> build() throws IOException {
      return new ParquetReader<T>(conf, file, readSupport, filter);
    }
  }
}
