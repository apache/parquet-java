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

import static parquet.Log.DEBUG;
import static parquet.bytes.BytesUtils.readIntLittleEndian;
import static parquet.hadoop.ParquetFileWriter.MAGIC;
import static parquet.hadoop.ParquetFileWriter.PARQUET_METADATA_FILE;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.mapred.Utils;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.format.PageHeader;
import parquet.format.Util;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactory.BytesDecompressor;
import parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
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

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  /**
   * for files provided, check if there's a summary file.
   * If a summary file is found it is used otherwise the file footer is used.
   * @param configuration the hadoop conf to connect to the file system;
   * @param partFiles the part files to read
   * @return the footers for those files using the summary file if possible.
   * @throws IOException
   */
  public static List<Footer> readAllFootersInParallelUsingSummaryFiles(final Configuration configuration, List<FileStatus> partFiles) throws IOException {

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
          // fileSystem is thread-safe
          FileSystem fileSystem = path.getFileSystem(configuration);
          Path summaryFile = new Path(path, PARQUET_METADATA_FILE);
          if (fileSystem.exists(summaryFile)) {
            if (Log.INFO) LOG.info("reading summary file: " + summaryFile);
            final List<Footer> footers = readSummaryFile(configuration, fileSystem.getFileStatus(summaryFile));
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
      List<Map<Path, Footer>> footersFromSummaries = runAllInParallel(5, summaries);
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
      result.addAll(readAllFootersInParallel(configuration, toRead));
    }

    return result;
  }

  private static <T> List<T> runAllInParallel(int parallelism, List<Callable<T>> toRun) throws ExecutionException {
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

  public static List<Footer> readAllFootersInParallel(final Configuration configuration, List<FileStatus> partFiles) throws IOException {
    List<Callable<Footer>> footers = new ArrayList<Callable<Footer>>();
    for (final FileStatus currentFile : partFiles) {
      footers.add(new Callable<Footer>() {
        @Override
        public Footer call() throws Exception {
          try {
            return new Footer(currentFile.getPath(), ParquetFileReader.readFooter(configuration, currentFile));
          } catch (IOException e) {
            throw new IOException("Could not read footer for file " + currentFile, e);
          }
        }
      });
    }
    try {
      return runAllInParallel(5, footers);
    } catch (ExecutionException e) {
      throw new IOException("Could not read footer: " + e.getMessage(), e.getCause());
    }
  }

  public static List<Footer> readAllFootersInParallel(Configuration configuration, FileStatus fileStatus) throws IOException {
    final FileSystem fs = fileStatus.getPath().getFileSystem(configuration);
    List<FileStatus> statuses;
    if (fileStatus.isDir()) {
      statuses = Arrays.asList(fs.listStatus(fileStatus.getPath(), new Utils.OutputFileUtils.OutputFilesFilter()));
    } else {
      statuses = new ArrayList<FileStatus>();
      statuses.add(fileStatus);
    }
    return readAllFootersInParallel(configuration, statuses);
  }

  public static List<Footer> readFooters(Configuration configuration, FileStatus pathStatus) throws IOException {
    try {
      if (pathStatus.isDir()) {
        Path summaryPath = new Path(pathStatus.getPath(), PARQUET_METADATA_FILE);
        FileSystem fs = summaryPath.getFileSystem(configuration);
        if (fs.exists(summaryPath)) {
          FileStatus summaryStatus = fs.getFileStatus(summaryPath);
          return readSummaryFile(configuration, summaryStatus);
        }
      }
    } catch (IOException e) {
      LOG.warn("can not read summary file for " + pathStatus.getPath(), e);
    }
    return readAllFootersInParallel(configuration, pathStatus);

  }

  public static List<Footer> readSummaryFile(Configuration configuration, FileStatus summaryStatus) throws IOException {
    final Path parent = summaryStatus.getPath().getParent();
    ParquetMetadata mergedFooters = readFooter(configuration, summaryStatus);
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
  public static final ParquetMetadata readFooter(Configuration configuration, Path file) throws IOException {
    FileSystem fileSystem = file.getFileSystem(configuration);
    return readFooter(configuration, fileSystem.getFileStatus(file));
  }


  public static final List<Footer> readFooters(Configuration configuration, Path file) throws IOException {
    FileSystem fileSystem = file.getFileSystem(configuration);
    return readFooters(configuration, fileSystem.getFileStatus(file));
  }

  /**
   * Reads the meta data block in the footer of the file
   * @param configuration
   * @param file the parquet File
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  public static final ParquetMetadata readFooter(Configuration configuration, FileStatus file) throws IOException {
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
      return parquetMetadataConverter.readParquetMetadata(f);
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
      List<Page> pagesInChunk = new ArrayList<Page>();
      DictionaryPage dictionaryPage = null;
      long valuesCountReadSoFar = 0;
      while (valuesCountReadSoFar < descriptor.metadata.getValueCount()) {
        PageHeader pageHeader = readPageHeader();
        switch (pageHeader.type) {
          case DICTIONARY_PAGE:
            // there is only one dictionary page per column chunk
            if (dictionaryPage != null) {
              throw new ParquetDecodingException("more than one dictionary page in column " + descriptor.col);
            }
            dictionaryPage =
                new DictionaryPage(
                    this.readAsBytesInput(pageHeader.compressed_page_size),
                    pageHeader.uncompressed_page_size,
                    pageHeader.dictionary_page_header.num_values,
                    parquetMetadataConverter.getEncoding(pageHeader.dictionary_page_header.encoding)
                    );
            break;
          case DATA_PAGE:
            pagesInChunk.add(
                new Page(
                    this.readAsBytesInput(pageHeader.compressed_page_size),
                    pageHeader.data_page_header.num_values,
                    pageHeader.uncompressed_page_size,
                    parquetMetadataConverter.fromParquetStatistics(pageHeader.data_page_header.statistics, descriptor.col.getType()),
                    parquetMetadataConverter.getEncoding(pageHeader.data_page_header.repetition_level_encoding),
                    parquetMetadataConverter.getEncoding(pageHeader.data_page_header.definition_level_encoding),
                    parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding)
                    ));
            valuesCountReadSoFar += pageHeader.data_page_header.num_values;
            break;
          default:
            if (DEBUG) LOG.debug("skipping page of type " + pageHeader.type + " of size " + pageHeader.compressed_page_size);
            this.skip(pageHeader.compressed_page_size);
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
