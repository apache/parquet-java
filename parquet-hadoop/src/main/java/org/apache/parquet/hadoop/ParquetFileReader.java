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
package org.apache.parquet.hadoop;

import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.DICTIONARY;
import static org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.STATISTICS;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.PARQUET_COMMON_METADATA_FILE;
import static org.apache.parquet.hadoop.ParquetFileWriter.PARQUET_METADATA_FILE;
import static org.apache.parquet.hadoop.ParquetInputFormat.DICTIONARY_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.DICTIONARY_FILTERING_ENABLED_DEFAULT;
import static org.apache.parquet.hadoop.ParquetInputFormat.STATS_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.STATS_FILTERING_ENABLED_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;

import org.apache.parquet.Log;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.CodecFactory.BytesDecompressor;
import org.apache.parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Internal implementation of the Parquet file reader as a block container
 *
 * @author Julien Le Dem
 *
 */
public class ParquetFileReader implements Closeable {

  private static final Log LOG = Log.getLog(ParquetFileReader.class);

  public static String PARQUET_READ_PARALLELISM = "parquet.metadata.read.parallelism";

  private static ParquetMetadataConverter converter = new ParquetMetadataConverter();

  /**
   * for files provided, check if there's a summary file.
   * If a summary file is found it is used otherwise the file footer is used.
   * @param configuration the hadoop conf to connect to the file system;
   * @param partFiles the part files to read
   * @return the footers for those files using the summary file if possible.
   * @throws IOException
   */
  @Deprecated
  public static List<Footer> readAllFootersInParallelUsingSummaryFiles(Configuration configuration, List<FileStatus> partFiles) throws IOException {
    return readAllFootersInParallelUsingSummaryFiles(configuration, partFiles, false);
  }

  private static MetadataFilter filter(boolean skipRowGroups) {
    return skipRowGroups ? SKIP_ROW_GROUPS : NO_FILTER;
  }

  /**
   * for files provided, check if there's a summary file.
   * If a summary file is found it is used otherwise the file footer is used.
   * @param configuration the hadoop conf to connect to the file system;
   * @param partFiles the part files to read
   * @param skipRowGroups to skipRowGroups in the footers
   * @return the footers for those files using the summary file if possible.
   * @throws IOException
   */
  public static List<Footer> readAllFootersInParallelUsingSummaryFiles(
      final Configuration configuration,
      final Collection<FileStatus> partFiles,
      final boolean skipRowGroups) throws IOException {

    // figure out list of all parents to part files
    Set<Path> parents = new HashSet<Path>();
    for (FileStatus part : partFiles) {
      parents.add(part.getPath().getParent());
    }

    // read corresponding summary files if they exist
    List<Callable<Map<Path, Footer>>> summaries = new ArrayList<Callable<Map<Path, Footer>>>();
    for (final Path path : parents) {
      summaries.add(new Callable<Map<Path, Footer>>() {
        @Override
        public Map<Path, Footer> call() throws Exception {
          ParquetMetadata mergedMetadata = readSummaryMetadata(configuration, path, skipRowGroups);
          if (mergedMetadata != null) {
            final List<Footer> footers;
            if (skipRowGroups) {
              footers = new ArrayList<Footer>();
              for (FileStatus f : partFiles) {
                footers.add(new Footer(f.getPath(), mergedMetadata));
              }
            } else {
              footers = footersFromSummaryFile(path, mergedMetadata);
            }
            Map<Path, Footer> map = new HashMap<Path, Footer>();
            for (Footer footer : footers) {
              // the folder may have been moved
              footer = new Footer(new Path(path, footer.getFile().getName()), footer.getParquetMetadata());
              map.put(footer.getFile(), footer);
            }
            return map;
          } else {
            return Collections.emptyMap();
          }
        }
      });
    }

    Map<Path, Footer> cache = new HashMap<Path, Footer>();
    try {
      List<Map<Path, Footer>> footersFromSummaries = runAllInParallel(configuration.getInt(PARQUET_READ_PARALLELISM, 5), summaries);
      for (Map<Path, Footer> footers : footersFromSummaries) {
        cache.putAll(footers);
      }
    } catch (ExecutionException e) {
      throw new IOException("Error reading summaries", e);
    }

    // keep only footers for files actually requested and read file footer if not found in summaries
    List<Footer> result = new ArrayList<Footer>(partFiles.size());
    List<FileStatus> toRead = new ArrayList<FileStatus>();
    for (FileStatus part : partFiles) {
      Footer f = cache.get(part.getPath());
      if (f != null) {
        result.add(f);
      } else {
        toRead.add(part);
      }
    }

    if (toRead.size() > 0) {
      // read the footers of the files that did not have a summary file
      if (Log.INFO) {
        LOG.info("reading another " + toRead.size() + " footers");
      }
      result.addAll(readAllFootersInParallel(configuration, toRead, skipRowGroups));
    }

    return result;
  }

  private static <T> List<T> runAllInParallel(int parallelism, List<Callable<T>> toRun) throws ExecutionException {
    LOG.info("Initiating action with parallelism: " + parallelism);
    ExecutorService threadPool = Executors.newFixedThreadPool(parallelism);
    try {
      List<Future<T>> futures = new ArrayList<Future<T>>();
      for (Callable<T> callable : toRun) {
        futures.add(threadPool.submit(callable));
      }
      List<T> result = new ArrayList<T>(toRun.size());
      for (Future<T> future : futures) {
        try {
          result.add(future.get());
        } catch (InterruptedException e) {
          throw new RuntimeException("The thread was interrupted", e);
        }
      }
      return result;
    } finally {
      threadPool.shutdownNow();
    }
  }

  @Deprecated
  public static List<Footer> readAllFootersInParallel(final Configuration configuration, List<FileStatus> partFiles) throws IOException {
    return readAllFootersInParallel(configuration, partFiles, false);
  }

  /**
   * read all the footers of the files provided
   * (not using summary files)
   * @param configuration the conf to access the File System
   * @param partFiles the files to read
   * @param skipRowGroups to skip the rowGroup info
   * @return the footers
   * @throws IOException
   */
  public static List<Footer> readAllFootersInParallel(final Configuration configuration, List<FileStatus> partFiles, final boolean skipRowGroups) throws IOException {
    List<Callable<Footer>> footers = new ArrayList<Callable<Footer>>();
    for (final FileStatus currentFile : partFiles) {
      footers.add(new Callable<Footer>() {
        @Override
        public Footer call() throws Exception {
          try {
            return new Footer(currentFile.getPath(), readFooter(configuration, currentFile, filter(skipRowGroups)));
          } catch (IOException e) {
            throw new IOException("Could not read footer for file " + currentFile, e);
          }
        }
      });
    }
    try {
      return runAllInParallel(configuration.getInt(PARQUET_READ_PARALLELISM, 5), footers);
    } catch (ExecutionException e) {
      throw new IOException("Could not read footer: " + e.getMessage(), e.getCause());
    }
  }

  /**
   * Read the footers of all the files under that path (recursively)
   * not using summary files.
   */
  public static List<Footer> readAllFootersInParallel(Configuration configuration, FileStatus fileStatus, boolean skipRowGroups) throws IOException {
    List<FileStatus> statuses = listFiles(configuration, fileStatus);
    return readAllFootersInParallel(configuration, statuses, skipRowGroups);
  }

  /**
   * Read the footers of all the files under that path (recursively)
   * not using summary files.
   * rowGroups are not skipped
   * @param configuration the configuration to access the FS
   * @param fileStatus the root dir
   * @return all the footers
   * @throws IOException
   */
  public static List<Footer> readAllFootersInParallel(Configuration configuration, FileStatus fileStatus) throws IOException {
    return readAllFootersInParallel(configuration, fileStatus, false);
  }


  @Deprecated
  public static List<Footer> readFooters(Configuration configuration, Path path) throws IOException {
    return readFooters(configuration, status(configuration, path));
  }

  private static FileStatus status(Configuration configuration, Path path) throws IOException {
    return path.getFileSystem(configuration).getFileStatus(path);
  }

  /**
   * this always returns the row groups
   * @param configuration
   * @param pathStatus
   * @return
   * @throws IOException
   */
  @Deprecated
  public static List<Footer> readFooters(Configuration configuration, FileStatus pathStatus) throws IOException {
    return readFooters(configuration, pathStatus, false);
  }

  /**
   * Read the footers of all the files under that path (recursively)
   * using summary files if possible
   * @param configuration the configuration to access the FS
   * @param pathStatus the root dir
   * @return all the footers
   * @throws IOException
   */
  public static List<Footer> readFooters(Configuration configuration, FileStatus pathStatus, boolean skipRowGroups) throws IOException {
    List<FileStatus> files = listFiles(configuration, pathStatus);
    return readAllFootersInParallelUsingSummaryFiles(configuration, files, skipRowGroups);
  }

  private static List<FileStatus> listFiles(Configuration conf, FileStatus fileStatus) throws IOException {
    if (fileStatus.isDir()) {
      FileSystem fs = fileStatus.getPath().getFileSystem(conf);
      FileStatus[] list = fs.listStatus(fileStatus.getPath(), HiddenFileFilter.INSTANCE);
      List<FileStatus> result = new ArrayList<FileStatus>();
      for (FileStatus sub : list) {
        result.addAll(listFiles(conf, sub));
      }
      return result;
    } else {
      return Arrays.asList(fileStatus);
    }
  }

  /**
   * Specifically reads a given summary file
   * @param configuration
   * @param summaryStatus
   * @return the metadata translated for each file
   * @throws IOException
   */
  public static List<Footer> readSummaryFile(Configuration configuration, FileStatus summaryStatus) throws IOException {
    final Path parent = summaryStatus.getPath().getParent();
    ParquetMetadata mergedFooters = readFooter(configuration, summaryStatus, filter(false));
    return footersFromSummaryFile(parent, mergedFooters);
  }

  static ParquetMetadata readSummaryMetadata(Configuration configuration, Path basePath, boolean skipRowGroups) throws IOException {
    Path metadataFile = new Path(basePath, PARQUET_METADATA_FILE);
    Path commonMetaDataFile = new Path(basePath, PARQUET_COMMON_METADATA_FILE);
    FileSystem fileSystem = basePath.getFileSystem(configuration);
    if (skipRowGroups && fileSystem.exists(commonMetaDataFile)) {
      // reading the summary file that does not contain the row groups
      if (Log.INFO) LOG.info("reading summary file: " + commonMetaDataFile);
      return readFooter(configuration, commonMetaDataFile, filter(skipRowGroups));
    } else if (fileSystem.exists(metadataFile)) {
      if (Log.INFO) {
        LOG.info("reading summary file: " + metadataFile);
      }
      return readFooter(configuration, metadataFile, filter(skipRowGroups));
    } else {
      return null;
    }
  }

  static List<Footer> footersFromSummaryFile(final Path parent, ParquetMetadata mergedFooters) {
    Map<Path, ParquetMetadata> footers = new HashMap<Path, ParquetMetadata>();
    List<BlockMetaData> blocks = mergedFooters.getBlocks();
    for (BlockMetaData block : blocks) {
      String path = block.getPath();
      Path fullPath = new Path(parent, path);
      ParquetMetadata current = footers.get(fullPath);
      if (current == null) {
        current = new ParquetMetadata(mergedFooters.getFileMetaData(), new ArrayList<BlockMetaData>());
        footers.put(fullPath, current);
      }
      current.getBlocks().add(block);
    }
    List<Footer> result = new ArrayList<Footer>();
    for (Entry<Path, ParquetMetadata> entry : footers.entrySet()) {
      result.add(new Footer(entry.getKey(), entry.getValue()));
    }
    return result;
  }

  /**
   * Reads the meta data block in the footer of the file
   * @param configuration
   * @param file the parquet File
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  @Deprecated
  public static final ParquetMetadata readFooter(Configuration configuration, Path file) throws IOException {
    return readFooter(configuration, file, NO_FILTER);
  }

  /**
   * Reads the meta data in the footer of the file.
   * Skipping row groups (or not) based on the provided filter
   * @param configuration
   * @param file the Parquet File
   * @param filter the filter to apply to row groups
   * @return the metadata with row groups filtered.
   * @throws IOException  if an error occurs while reading the file
   */
  public static ParquetMetadata readFooter(Configuration configuration, Path file, MetadataFilter filter) throws IOException {
    FileSystem fileSystem = file.getFileSystem(configuration);
    return readFooter(configuration, fileSystem.getFileStatus(file), filter);
  }

  /**
   * @deprecated use {@link ParquetFileReader#readFooter(Configuration, FileStatus, MetadataFilter)}
   */
  @Deprecated
  public static final ParquetMetadata readFooter(Configuration configuration, FileStatus file) throws IOException {
    return readFooter(configuration, file, NO_FILTER);
  }

  /**
   * Reads the meta data block in the footer of the file
   * @param configuration
   * @param file the parquet File
   * @param filter the filter to apply to row groups
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  public static final ParquetMetadata readFooter(Configuration configuration, FileStatus file, MetadataFilter filter) throws IOException {
    FileSystem fileSystem = file.getPath().getFileSystem(configuration);
    SeekableInputStream in = HadoopStreams.wrap(fileSystem.open(file.getPath()));
    try {
      return readFooter(file.getLen(), file.getPath().toString(), in, filter);
    } finally {
      in.close();
    }
  }

  /**
   * Reads the meta data block in the footer of the file using provided input stream
   * @param fileLen length of the file
   * @param filePath file location
   * @param f input stream for the file
   * @param filter the filter to apply to row groups
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  public static final ParquetMetadata readFooter(long fileLen, String filePath, SeekableInputStream f, MetadataFilter filter) throws IOException {
    if (Log.DEBUG) {
      LOG.debug("File length " + fileLen);
    }
    int FOOTER_LENGTH_SIZE = 4;
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException(filePath + " is not a Parquet file (too small)");
    }
    long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;
    if (Log.DEBUG) {
      LOG.debug("reading footer index at " + footerLengthIndex);
    }

    f.seek(footerLengthIndex);
    int footerLength = readIntLittleEndian(f);
    byte[] magic = new byte[MAGIC.length];
    f.readFully(magic);
    if (!Arrays.equals(MAGIC, magic)) {
      throw new RuntimeException(filePath + " is not a Parquet file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
    }
    long footerIndex = footerLengthIndex - footerLength;
    if (Log.DEBUG) {
      LOG.debug("read footer length: " + footerLength + ", footer index: " + footerIndex);
    }
    if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
      throw new RuntimeException("corrupted file: the footer index is not within the file");
    }
    f.seek(footerIndex);
    return converter.readParquetMetadata(f, filter);
  }

  public static ParquetFileReader open(Configuration conf, Path file) throws IOException {
    return new ParquetFileReader(conf, file);
  }

  public static ParquetFileReader open(Configuration conf, Path file, MetadataFilter filter) throws IOException {
    return new ParquetFileReader(conf, file, filter);
  }

  public static ParquetFileReader open(Configuration conf, Path file, ParquetMetadata footer) throws IOException {
    return new ParquetFileReader(conf, file, footer);
  }

  private final CodecFactory codecFactory;
  private final SeekableInputStream f;
  private final FileStatus fileStatus;
  private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<ColumnPath, ColumnDescriptor>();
  private final FileMetaData fileMetaData; // may be null
  private final ByteBufferAllocator allocator;
  private final Configuration conf;

  // not final. in some cases, this may be lazily loaded for backward-compat.
  private ParquetMetadata footer;
  // blocks can be filtered after they are read (or set in the constructor)
  private List<BlockMetaData> blocks;

  private int currentBlock = 0;
  private ColumnChunkPageReadStore currentRowGroup = null;
  private DictionaryPageReader nextDictionaryReader = null;

  /**
   * @deprecated use @link{ParquetFileReader(Configuration configuration, FileMetaData fileMetaData,
   * Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns)} instead
   */
  public ParquetFileReader(Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
    this(configuration, null, filePath, blocks, columns);
  }

  /**
   * @param configuration the Hadoop conf
   * @param fileMetaData fileMetaData for parquet file
   * @param blocks the blocks to read
   * @param columns the columns to read (their path)
   * @throws IOException if the file can not be opened
   */
  @Deprecated
  public ParquetFileReader(
      Configuration configuration, FileMetaData fileMetaData,
      Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
    this.conf = configuration;
    this.fileMetaData = fileMetaData;
    FileSystem fs = filePath.getFileSystem(configuration);
    this.f = HadoopStreams.wrap(fs.open(filePath));
    this.fileStatus = fs.getFileStatus(filePath);
    this.blocks = blocks;
    for (ColumnDescriptor col : columns) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }
    // the page size parameter isn't meaningful when only using
    // the codec factory to get decompressors
    this.codecFactory = new CodecFactory(configuration, 0);
    this.allocator = new HeapByteBufferAllocator();
  }

  /**
   * @param configuration the Hadoop Configuration
   * @param file Path to a parquet file
   * @throws IOException if the file can not be opened
   */
  private ParquetFileReader(Configuration configuration, Path file) throws IOException {
    this(configuration, file, NO_FILTER);
  }

  /**
   * @param conf the Hadoop Configuration
   * @param file Path to a parquet file
   * @param filter a {@link MetadataFilter} for selecting row groups
   * @throws IOException if the file can not be opened
   */
  public ParquetFileReader(Configuration conf, Path file, MetadataFilter filter) throws IOException {
    this.conf = conf;
    FileSystem fs = file.getFileSystem(conf);
    this.fileStatus = fs.getFileStatus(file);
    this.f = HadoopStreams.wrap(fs.open(file));
    this.footer = readFooter(fileStatus.getLen(), fileStatus.getPath().toString(), f, filter);
    this.fileMetaData = footer.getFileMetaData();
    this.blocks = footer.getBlocks();
    for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }
    // the page size parameter isn't meaningful when only using
    // the codec factory to get decompressors
    this.codecFactory = new CodecFactory(conf, 0);
    this.allocator = new HeapByteBufferAllocator();
  }

  /**
   * @param conf the Hadoop Configuration
   * @param file Path to a parquet file
   * @param footer a {@link ParquetMetadata} footer already read from the file
   * @throws IOException if the file can not be opened
   */
  public ParquetFileReader(Configuration conf, Path file, ParquetMetadata footer) throws IOException {
    this.conf = conf;
    FileSystem fs = file.getFileSystem(conf);
    this.fileStatus = fs.getFileStatus(file);
    this.f = HadoopStreams.wrap(fs.open(file));
    this.footer = footer;
    this.fileMetaData = footer.getFileMetaData();
    this.blocks = footer.getBlocks();
    for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }
    // the page size parameter isn't meaningful when only using
    // the codec factory to get decompressors
    this.codecFactory = new CodecFactory(conf, 0);
    this.allocator = new HeapByteBufferAllocator();
  }

  public ParquetMetadata getFooter() {
    if (footer == null) {
      try {
        // don't read the row groups because this.blocks is always set
        this.footer = readFooter(fileStatus.getLen(), fileStatus.getPath().toString(), f, SKIP_ROW_GROUPS);
      } catch (IOException e) {
        throw new ParquetDecodingException("Unable to read file footer", e);
      }
    }
    return footer;
  }

  public FileMetaData getFileMetaData() {
    if (fileMetaData != null) {
      return fileMetaData;
    }
    return getFooter().getFileMetaData();
  }

  public long getRecordCount() {
    long total = 0;
    for (BlockMetaData block : blocks) {
      total += block.getRowCount();
    }
    return total;
  }

  public Path getPath() {
    return fileStatus.getPath();
  }

  void filterRowGroups(FilterCompat.Filter filter) throws IOException {
    // set up data filters based on configured levels
    List<RowGroupFilter.FilterLevel> levels = new ArrayList<RowGroupFilter.FilterLevel>();

    if (conf.getBoolean(
        STATS_FILTERING_ENABLED, STATS_FILTERING_ENABLED_DEFAULT)) {
      levels.add(STATISTICS);
    }

    if (conf.getBoolean(
        DICTIONARY_FILTERING_ENABLED, DICTIONARY_FILTERING_ENABLED_DEFAULT)) {
      levels.add(DICTIONARY);
    }

    this.blocks = RowGroupFilter.filterRowGroups(levels, filter, blocks, this);
  }

  public List<BlockMetaData> getRowGroups() {
    return blocks;
  }

  public void appendTo(ParquetFileWriter writer) throws IOException {
    writer.appendRowGroups(f, blocks, true);
  }

  /**
   * Reads all the columns requested from the row group at the current file position.
   * @throws IOException if an error occurs while reading
   * @return the PageReadStore which can provide PageReaders for each column.
   */
  public PageReadStore readNextRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    this.currentRowGroup = new ColumnChunkPageReadStore(block.getRowCount());
    // prepare the list of consecutive chunks to read them in one scan
    List<ConsecutiveChunkList> allChunks = new ArrayList<ConsecutiveChunkList>();
    ConsecutiveChunkList currentChunks = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      BenchmarkCounter.incrementTotalBytes(mc.getTotalSize());
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        long startingPos = mc.getStartingPos();
        // first chunk or not consecutive => new list
        if (currentChunks == null || currentChunks.endPos() != startingPos) {
          currentChunks = new ConsecutiveChunkList(startingPos);
          allChunks.add(currentChunks);
        }
        currentChunks.addChunk(new ChunkDescriptor(columnDescriptor, mc, startingPos, (int)mc.getTotalSize()));
      }
    }
    // actually read all the chunks
    for (ConsecutiveChunkList consecutiveChunks : allChunks) {
      final List<Chunk> chunks = consecutiveChunks.readAll(f);
      for (Chunk chunk : chunks) {
        currentRowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
      }
    }

    // avoid re-reading bytes the dictionary reader is used after this call
    if (nextDictionaryReader != null) {
      nextDictionaryReader.setRowGroup(currentRowGroup);
    }

    advanceToNextBlock();

    return currentRowGroup;
  }

  public boolean skipNextRowGroup() {
    return advanceToNextBlock();
  }

  private boolean advanceToNextBlock() {
    if (currentBlock == blocks.size()) {
      return false;
    }

    // update the current block and instantiate a dictionary reader for it
    ++currentBlock;
    this.nextDictionaryReader = null;

    return true;
  }

  /**
   * Returns a {@link DictionaryPageReadStore} for the row group that would be
   * returned by calling {@link #readNextRowGroup()} or skipped by calling
   * {@link #skipNextRowGroup()}.
   *
   * @return a DictionaryPageReadStore for the next row group
   */
  public DictionaryPageReadStore getNextDictionaryReader() {
    if (nextDictionaryReader == null && currentBlock < blocks.size()) {
      this.nextDictionaryReader = getDictionaryReader(blocks.get(currentBlock));
    }
    return nextDictionaryReader;
  }

  public DictionaryPageReader getDictionaryReader(BlockMetaData block) {
    return new DictionaryPageReader(this, block);
  }

  /**
   * Reads and decompresses a dictionary page for the given column chunk.
   *
   * Returns null if the given column chunk has no dictionary page.
   *
   * @param meta a column's ColumnChunkMetaData to read the dictionary from
   * @return an uncompressed DictionaryPage or null
   * @throws IOException
   */
  DictionaryPage readDictionary(ColumnChunkMetaData meta) throws IOException {
    if (!meta.getEncodings().contains(Encoding.PLAIN_DICTIONARY) &&
        !meta.getEncodings().contains(Encoding.RLE_DICTIONARY)) {
      return null;
    }

    // TODO: this should use getDictionaryPageOffset() but it isn't reliable.
    if (f.getPos() != meta.getStartingPos()) {
      f.seek(meta.getStartingPos());
    }

    PageHeader pageHeader = Util.readPageHeader(f);
    if (!pageHeader.isSetDictionary_page_header()) {
      return null; // TODO: should this complain?
    }

    DictionaryPage compressedPage = readCompressedDictionary(pageHeader, f);
    BytesDecompressor decompressor = codecFactory.getDecompressor(meta.getCodec());

    return new DictionaryPage(
        decompressor.decompress(compressedPage.getBytes(), compressedPage.getUncompressedSize()),
        compressedPage.getDictionarySize(),
        compressedPage.getEncoding());
  }

  private static DictionaryPage readCompressedDictionary(
      PageHeader pageHeader, SeekableInputStream fin) throws IOException {
    DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();

    int uncompressedPageSize = pageHeader.getUncompressed_page_size();
    int compressedPageSize = pageHeader.getCompressed_page_size();

    byte [] dictPageBytes = new byte[compressedPageSize];
    fin.readFully(dictPageBytes);

    BytesInput bin = BytesInput.from(dictPageBytes);

    return new DictionaryPage(
        bin, uncompressedPageSize, dictHeader.getNum_values(),
        converter.getEncoding(dictHeader.getEncoding()));
  }

  @Override
  public void close() throws IOException {
    try {
      if (f != null) {
        f.close();
      }
    } finally {
      if (codecFactory != null) {
        codecFactory.release();
      }
    }
  }

  /**
   * The data for a column chunk
   *
   * @author Julien Le Dem
   *
   */
  private class Chunk extends ByteBufferInputStream {

    private final ChunkDescriptor descriptor;

    /**
     *
     * @param descriptor descriptor for the chunk
     * @param data contains the chunk data at offset
     * @param offset where the chunk starts in offset
     */
    public Chunk(ChunkDescriptor descriptor, ByteBuffer data, int offset) {
      super(data, offset, descriptor.size);
      this.descriptor = descriptor;
    }

    protected PageHeader readPageHeader() throws IOException {
      return Util.readPageHeader(this);
    }

    /**
     * Read all of the pages in a given column chunk.
     * @return the list of pages
     */
    public ColumnChunkPageReader readAllPages() throws IOException {
      List<DataPage> pagesInChunk = new ArrayList<DataPage>();
      DictionaryPage dictionaryPage = null;
      long valuesCountReadSoFar = 0;
      while (valuesCountReadSoFar < descriptor.metadata.getValueCount()) {
        PageHeader pageHeader = readPageHeader();
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();
        switch (pageHeader.type) {
          case DICTIONARY_PAGE:
            // there is only one dictionary page per column chunk
            if (dictionaryPage != null) {
              throw new ParquetDecodingException("more than one dictionary page in column " + descriptor.col);
            }
          DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
          dictionaryPage =
                new DictionaryPage(
                    this.readAsBytesInput(compressedPageSize),
                    uncompressedPageSize,
                    dicHeader.getNum_values(),
                    converter.getEncoding(dicHeader.getEncoding())
                    );
            break;
          case DATA_PAGE:
            DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
            pagesInChunk.add(
                new DataPageV1(
                    this.readAsBytesInput(compressedPageSize),
                    dataHeaderV1.getNum_values(),
                    uncompressedPageSize,
                    fromParquetStatistics(
                        getFileMetaData().getCreatedBy(),
                        dataHeaderV1.getStatistics(),
                        descriptor.col.getType()),
                    converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                    converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                    converter.getEncoding(dataHeaderV1.getEncoding())
                    ));
            valuesCountReadSoFar += dataHeaderV1.getNum_values();
            break;
          case DATA_PAGE_V2:
            DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
            int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
            pagesInChunk.add(
                new DataPageV2(
                    dataHeaderV2.getNum_rows(),
                    dataHeaderV2.getNum_nulls(),
                    dataHeaderV2.getNum_values(),
                    this.readAsBytesInput(dataHeaderV2.getRepetition_levels_byte_length()),
                    this.readAsBytesInput(dataHeaderV2.getDefinition_levels_byte_length()),
                    converter.getEncoding(dataHeaderV2.getEncoding()),
                    this.readAsBytesInput(dataSize),
                    uncompressedPageSize,
                    fromParquetStatistics(
                        getFileMetaData().getCreatedBy(),
                        dataHeaderV2.getStatistics(),
                        descriptor.col.getType()),
                    dataHeaderV2.isIs_compressed()
                    ));
            valuesCountReadSoFar += dataHeaderV2.getNum_values();
            break;
          default:
            if (DEBUG) {
              LOG.debug("skipping page of type " + pageHeader.getType() + " of size " + compressedPageSize);
            }
            this.skip(compressedPageSize);
            break;
        }
      }
      if (valuesCountReadSoFar != descriptor.metadata.getValueCount()) {
        // Would be nice to have a CorruptParquetFileException or something as a subclass?
        throw new IOException(
            "Expected " + descriptor.metadata.getValueCount() + " values in column chunk at " +
            getPath() + " offset " + descriptor.metadata.getFirstDataPageOffset() +
            " but got " + valuesCountReadSoFar + " values instead over " + pagesInChunk.size()
            + " pages ending at file offset " + (descriptor.fileOffset + pos()));
      }
      BytesDecompressor decompressor = codecFactory.getDecompressor(descriptor.metadata.getCodec());
      return new ColumnChunkPageReader(decompressor, pagesInChunk, dictionaryPage);
    }

    /**
     * @return the current position in the chunk
     */
    public int pos() {
      return this.byteBuf.position();
    }

    /**
     * @param size the size of the page
     * @return the page
     * @throws IOException
     */
    public BytesInput readAsBytesInput(int size) throws IOException {
      int pos = this.byteBuf.position();
      final BytesInput r = BytesInput.from(this.byteBuf, pos, size);
      this.byteBuf.position(pos + size);
      return r;
    }

  }

  /**
   * deals with a now fixed bug where compressedLength was missing a few bytes.
   *
   * @author Julien Le Dem
   *
   */
  private class WorkaroundChunk extends Chunk {

    private final SeekableInputStream f;

    /**
     * @param descriptor the descriptor of the chunk
     * @param byteBuf contains the data of the chunk at offset
     * @param offset where the chunk starts in data
     * @param f the file stream positioned at the end of this chunk
     */
    private WorkaroundChunk(ChunkDescriptor descriptor, ByteBuffer byteBuf, int offset, SeekableInputStream f) {
      super(descriptor, byteBuf, offset);
      this.f = f;
    }

    protected PageHeader readPageHeader() throws IOException {
      PageHeader pageHeader;
      int initialPos = pos();
      try {
        pageHeader = Util.readPageHeader(this);
      } catch (IOException e) {
        // this is to workaround a bug where the compressedLength
        // of the chunk is missing the size of the header of the dictionary
        // to allow reading older files (using dictionary) we need this.
        // usually 13 to 19 bytes are missing
        // if the last page is smaller than this, the page header itself is truncated in the buffer.
        this.byteBuf.position(initialPos); // resetting the buffer to the position before we got the error
        LOG.info("completing the column chunk to read the page header");
        pageHeader = Util.readPageHeader(new SequenceInputStream(this, f)); // trying again from the buffer + remainder of the stream.
      }
      return pageHeader;
    }

    public BytesInput readAsBytesInput(int size) throws IOException {
      if (pos() + size > initPos + count) {
        // this is to workaround a bug where the compressedLength
        // of the chunk is missing the size of the header of the dictionary
        // to allow reading older files (using dictionary) we need this.
        // usually 13 to 19 bytes are missing
        int l1 = initPos + count - pos();
        int l2 = size - l1;
        LOG.info("completed the column chunk with " + l2 + " bytes");
        return BytesInput.concat(super.readAsBytesInput(l1), BytesInput.copy(BytesInput.from(f, l2)));
      }
      return super.readAsBytesInput(size);
    }

  }


  /**
   * information needed to read a column chunk
   */
  private static class ChunkDescriptor {

    private final ColumnDescriptor col;
    private final ColumnChunkMetaData metadata;
    private final long fileOffset;
    private final int size;

    /**
     * @param col column this chunk is part of
     * @param metadata metadata for the column
     * @param fileOffset offset in the file where this chunk starts
     * @param size size of the chunk
     */
    private ChunkDescriptor(
        ColumnDescriptor col,
        ColumnChunkMetaData metadata,
        long fileOffset,
        int size) {
      super();
      this.col = col;
      this.metadata = metadata;
      this.fileOffset = fileOffset;
      this.size = size;
    }
  }

  /**
   * describes a list of consecutive column chunks to be read at once.
   *
   * @author Julien Le Dem
   */
  private class ConsecutiveChunkList {

    private final long offset;
    private int length;
    private final List<ChunkDescriptor> chunks = new ArrayList<ChunkDescriptor>();

    /**
     * @param offset where the first chunk starts
     */
    ConsecutiveChunkList(long offset) {
      this.offset = offset;
    }

    /**
     * adds a chunk to the list.
     * It must be consecutive to the previous chunk
     * @param descriptor
     */
    public void addChunk(ChunkDescriptor descriptor) {
      chunks.add(descriptor);
      length += descriptor.size;
    }

    /**
     * @param f file to read the chunks from
     * @return the chunks
     * @throws IOException
     */
    public List<Chunk> readAll(SeekableInputStream f) throws IOException {
      List<Chunk> result = new ArrayList<Chunk>(chunks.size());
      f.seek(offset);

      // Allocate the bytebuffer based on whether the FS can support it.
      ByteBuffer chunksByteBuffer = allocator.allocate(length);
      f.readFully(chunksByteBuffer);

      // report in a counter the data we just scanned
      BenchmarkCounter.incrementBytesRead(length);
      int currentChunkOffset = 0;
      for (int i = 0; i < chunks.size(); i++) {
        ChunkDescriptor descriptor = chunks.get(i);
        if (i < chunks.size() - 1) {
          result.add(new Chunk(descriptor, chunksByteBuffer, currentChunkOffset));
        } else {
          // because of a bug, the last chunk might be larger than descriptor.size
          result.add(new WorkaroundChunk(descriptor, chunksByteBuffer, currentChunkOffset, f));
        }
        currentChunkOffset += descriptor.size;
      }
      return result ;
    }

    /**
     * @return the position following the last byte of these chunks
     */
    public long endPos() {
      return offset + length;
    }

  }

}
