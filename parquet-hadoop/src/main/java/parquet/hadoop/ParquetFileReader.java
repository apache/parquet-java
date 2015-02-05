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

import static parquet.Log.DEBUG;
import static parquet.bytes.BytesUtils.readIntLittleEndian;
import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS;
import static parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics;
import static parquet.hadoop.ParquetFileWriter.MAGIC;
import static parquet.hadoop.ParquetFileWriter.PARQUET_COMMON_METADATA_FILE;
import static parquet.hadoop.ParquetFileWriter.PARQUET_METADATA_FILE;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.SequenceInputStream;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageReadStore;
import parquet.common.schema.ColumnPath;
import parquet.format.DataPageHeader;
import parquet.format.DataPageHeaderV2;
import parquet.format.DictionaryPageHeader;
import parquet.format.PageHeader;
import parquet.format.Util;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import parquet.hadoop.CodecFactory.BytesDecompressor;
import parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.counters.BenchmarkCounter;
import parquet.io.ParquetDecodingException;

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
      if (Log.INFO) LOG.info("reading another " + toRead.size() + " footers");
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
   * rowGroups are not skipped
   * @param configuration the configuration to access the FS
   * @param fileStatus the root dir
   * @return all the footers
   * @throws IOException
   */
  public static List<Footer> readAllFootersInParallel(Configuration configuration, FileStatus fileStatus) throws IOException {
    List<FileStatus> statuses = listFiles(configuration, fileStatus);
    return readAllFootersInParallel(configuration, statuses, false);
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
   * @param fileStatus the root dir
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
      FileStatus[] list = fs.listStatus(fileStatus.getPath(), new PathFilter() {
        @Override
        public boolean accept(Path p) {
          return !p.getName().startsWith("_") && !p.getName().startsWith(".");
        }
      });
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
      if (Log.INFO) LOG.info("reading summary file: " + metadataFile);
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
    FSDataInputStream f = fileSystem.open(file.getPath());
    try {
      long l = file.getLen();
      if (Log.DEBUG) LOG.debug("File length " + l);
      int FOOTER_LENGTH_SIZE = 4;
      if (l < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
        throw new RuntimeException(file.getPath() + " is not a Parquet file (too small)");
      }
      long footerLengthIndex = l - FOOTER_LENGTH_SIZE - MAGIC.length;
      if (Log.DEBUG) LOG.debug("reading footer index at " + footerLengthIndex);

      f.seek(footerLengthIndex);
      int footerLength = readIntLittleEndian(f);
      byte[] magic = new byte[MAGIC.length];
      f.readFully(magic);
      if (!Arrays.equals(MAGIC, magic)) {
        throw new RuntimeException(file.getPath() + " is not a Parquet file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
      }
      long footerIndex = footerLengthIndex - footerLength;
      if (Log.DEBUG) LOG.debug("read footer length: " + footerLength + ", footer index: " + footerIndex);
      if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
        throw new RuntimeException("corrupted file: the footer index is not within the file");
      }
      f.seek(footerIndex);
      return converter.readParquetMetadata(f, filter);
    } finally {
      f.close();
    }
  }

  private final CodecFactory codecFactory;
  private final List<BlockMetaData> blocks;
  private final FSDataInputStream f;
  private final Path filePath;
  private int currentBlock = 0;
  private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<ColumnPath, ColumnDescriptor>();

  /**
   * @param f the Parquet file (will be opened for read in this constructor)
   * @param blocks the blocks to read
   * @param colums the columns to read (their path)
   * @param codecClassName the codec used to compress the blocks
   * @throws IOException if the file can not be opened
   */
  public ParquetFileReader(Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
    this.filePath = filePath;
    FileSystem fs = filePath.getFileSystem(configuration);
    this.f = fs.open(filePath);
    this.blocks = blocks;
    for (ColumnDescriptor col : columns) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }
    this.codecFactory = new CodecFactory(configuration);
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
    ColumnChunkPageReadStore columnChunkPageReadStore = new ColumnChunkPageReadStore(block.getRowCount());
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
        columnChunkPageReadStore.addColumn(chunk.descriptor.col, chunk.readAllPages());
      }
    }
    ++currentBlock;
    return columnChunkPageReadStore;
  }



  @Override
  public void close() throws IOException {
    f.close();
    this.codecFactory.release();
  }

  /**
   * The data for a column chunk
   *
   * @author Julien Le Dem
   *
   */
  private class Chunk extends ByteArrayInputStream {

    private final ChunkDescriptor descriptor;

    /**
     *
     * @param descriptor descriptor for the chunk
     * @param data contains the chunk data at offset
     * @param offset where the chunk starts in offset
     */
    public Chunk(ChunkDescriptor descriptor, byte[] data, int offset) {
      super(data);
      this.descriptor = descriptor;
      this.pos = offset;
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
                    fromParquetStatistics(dataHeaderV1.getStatistics(), descriptor.col.getType()),
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
                    fromParquetStatistics(dataHeaderV2.getStatistics(), descriptor.col.getType()),
                    dataHeaderV2.isIs_compressed()
                    ));
            valuesCountReadSoFar += dataHeaderV2.getNum_values();
            break;
          default:
            if (DEBUG) LOG.debug("skipping page of type " + pageHeader.getType() + " of size " + compressedPageSize);
            this.skip(compressedPageSize);
            break;
        }
      }
      if (valuesCountReadSoFar != descriptor.metadata.getValueCount()) {
        // Would be nice to have a CorruptParquetFileException or something as a subclass?
        throw new IOException(
            "Expected " + descriptor.metadata.getValueCount() + " values in column chunk at " +
            filePath + " offset " + descriptor.metadata.getFirstDataPageOffset() +
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
      return this.pos;
    }

    /**
     * @param size the size of the page
     * @return the page
     * @throws IOException
     */
    public BytesInput readAsBytesInput(int size) throws IOException {
      final BytesInput r = BytesInput.from(this.buf, this.pos, size);
      this.pos += size;
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

    private final FSDataInputStream f;

    /**
     * @param descriptor the descriptor of the chunk
     * @param data contains the data of the chunk at offset
     * @param offset where the chunk starts in data
     * @param f the file stream positioned at the end of this chunk
     */
    private WorkaroundChunk(ChunkDescriptor descriptor, byte[] data, int offset, FSDataInputStream f) {
      super(descriptor, data, offset);
      this.f = f;
    }

    protected PageHeader readPageHeader() throws IOException {
      PageHeader pageHeader;
      int initialPos = this.pos;
      try {
        pageHeader = Util.readPageHeader(this);
      } catch (IOException e) {
        // this is to workaround a bug where the compressedLength
        // of the chunk is missing the size of the header of the dictionary
        // to allow reading older files (using dictionary) we need this.
        // usually 13 to 19 bytes are missing
        // if the last page is smaller than this, the page header itself is truncated in the buffer.
        this.pos = initialPos; // resetting the buffer to the position before we got the error
        LOG.info("completing the column chunk to read the page header");
        pageHeader = Util.readPageHeader(new SequenceInputStream(this, f)); // trying again from the buffer + remainder of the stream.
      }
      return pageHeader;
    }

    public BytesInput readAsBytesInput(int size) throws IOException {
      if (pos + size > count) {
        // this is to workaround a bug where the compressedLength
        // of the chunk is missing the size of the header of the dictionary
        // to allow reading older files (using dictionary) we need this.
        // usually 13 to 19 bytes are missing
        int l1 = count - pos;
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
    public List<Chunk> readAll(FSDataInputStream f) throws IOException {
      List<Chunk> result = new ArrayList<Chunk>(chunks.size());
      f.seek(offset);
      byte[] chunksBytes = new byte[length];
      f.readFully(chunksBytes);
      // report in a counter the data we just scanned
      BenchmarkCounter.incrementBytesRead(length);
      int currentChunkOffset = 0;
      for (int i = 0; i < chunks.size(); i++) {
        ChunkDescriptor descriptor = chunks.get(i);
        if (i < chunks.size() - 1) {
          result.add(new Chunk(descriptor, chunksBytes, currentChunkOffset));
        } else {
          // because of a bug, the last chunk might be larger than descriptor.size
          result.add(new WorkaroundChunk(descriptor, chunksBytes, currentChunkOffset, f));
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
