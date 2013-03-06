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
import static parquet.hadoop.ParquetFileWriter.PARQUET_SUMMARY;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import parquet.column.mem.Page;
import parquet.column.mem.PageReadStore;
import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactory.BytesDecompressor;
import parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;

/**
 * Reads a Parquet file
 *
 * @author Julien Le Dem
 *
 */
public class ParquetFileReader {

  private static final Log LOG = Log.getLog(ParquetFileReader.class);

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static ParquetMetadata deserializeFooter(InputStream is) throws IOException {
    parquet.format.FileMetaData parquetMetadata = parquetMetadataConverter.readFileMetaData(is);
    if (Log.DEBUG) LOG.debug(parquetMetadataConverter.toString(parquetMetadata));
    ParquetMetadata metadata = parquetMetadataConverter.fromParquetMetadata(parquetMetadata);
    if (Log.DEBUG) LOG.debug(ParquetMetadata.toPrettyJSON(metadata));
    return metadata;
  }

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
    Map<Path, Footer> cache = new HashMap<Path, Footer>();
    for (Path path : parents) {
      FileSystem fileSystem = path.getFileSystem(configuration);
      Path summaryFile = new Path(path, PARQUET_SUMMARY);
      if (fileSystem.exists(summaryFile)) {
        if (Log.INFO) LOG.info("reading summary file: " + summaryFile);
        List<Footer> footers = readSummaryFile(configuration, fileSystem.getFileStatus(summaryFile));
        for (Footer footer : footers) {
          // the folder may have been moved
          footer = new Footer(new Path(path, footer.getFile().getName()), footer.getParquetMetadata());
          cache.put(footer.getFile(), footer);
        }
      }
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

  public static List<Footer> readAllFootersInParallel(final Configuration configuration, List<FileStatus> partFiles) throws IOException {
    ExecutorService threadPool = Executors.newFixedThreadPool(5);
    try {
      List<Future<Footer>> footers = new ArrayList<Future<Footer>>();
      for (final FileStatus currentFile : partFiles) {
        footers.add(threadPool.submit(new Callable<Footer>() {
          @Override
          public Footer call() throws Exception {
            try {
              FileSystem fs = currentFile.getPath().getFileSystem(configuration);
              return readFooter(fs, currentFile);
            } catch (IOException e) {
              throw new IOException("Could not read footer for file " + currentFile, e);
            }
          }

          private Footer readFooter(final FileSystem fs,
              final FileStatus currentFile) throws IOException {
            ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(configuration, currentFile);
            return new Footer(currentFile.getPath(), parquetMetadata);
          }
        }));
      }
      List<Footer> result = new ArrayList<Footer>(footers.size());
      for (Future<Footer> futureFooter : footers) {
        try {
          result.add(futureFooter.get());
        } catch (InterruptedException e) {
          Thread.interrupted();
          throw new RuntimeException("The thread was interrupted", e);
        } catch (ExecutionException e) {
          throw new IOException("Could not read footer: " + e.getMessage(), e.getCause());
        }
      }
      return result;
    } finally {
      threadPool.shutdownNow();
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
        Path summaryPath = new Path(pathStatus.getPath(), PARQUET_SUMMARY);
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
    FileSystem fs = summaryStatus.getPath().getFileSystem(configuration);
    FSDataInputStream summary = fs.open(summaryStatus.getPath());
    int footerCount = summary.readInt();
    List<Footer> result = new ArrayList<Footer>(footerCount);
    for (int i = 0; i < footerCount; i++) {
      Path file = new Path(summary.readUTF());
      ParquetMetadata parquetMetaData = parquetMetadataConverter.fromParquetMetadata(parquetMetadataConverter.readFileMetaData(summary));
      result.add(new Footer(file, parquetMetaData));
    }
    summary.close();
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
    long l = file.getLen();
    if (Log.DEBUG) LOG.debug("File length " + l);
    int FOOTER_LENGTH_SIZE = 4;
    if (l <= MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
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
    return deserializeFooter(f);

  }
  private CodecFactory codecFactory;

  private final List<BlockMetaData> blocks;
  private final FSDataInputStream f;
  private final Path filePath;
  private int currentBlock = 0;
  private Map<String, ColumnDescriptor> paths = new HashMap<String, ColumnDescriptor>();

  /**
   *
   * @param f the Parquet file
   * @param blocks the blocks to read
   * @param colums the columns to read (their path)
   * @param codecClassName the codec used to compress the blocks
   * @throws IOException if the file can not be opened
   */
  public ParquetFileReader(Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
    FileSystem fs = FileSystem.get(configuration);
    this.filePath = filePath;
    this.f = fs.open(filePath);
    this.blocks = blocks;
    for (ColumnDescriptor col : columns) {
      paths.put(Arrays.toString(col.getPath()), col);
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
    for (ColumnChunkMetaData mc : block.getColumns()) {
      String pathKey = Arrays.toString(mc.getPath());
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        List<Page> pagesInChunk = readColumnChunkPages(columnDescriptor, mc);
        BytesDecompressor decompressor = codecFactory.getDecompressor(mc.getCodec());
        ColumnChunkPageReader columnChunkPageReader = new ColumnChunkPageReader(decompressor, pagesInChunk);
        columnChunkPageReadStore.addColumn(columnDescriptor, columnChunkPageReader);
      }
    }
    ++currentBlock;
    return columnChunkPageReadStore;
  }

  /**
   * Read all of the pages in a given column chunk.
   * @return the list of pages
   */
  private List<Page> readColumnChunkPages(ColumnDescriptor columnDescriptor, ColumnChunkMetaData metadata)
      throws IOException {
    f.seek(metadata.getFirstDataPageOffset());
    if (DEBUG) {
      LOG.debug(f.getPos() + ": start column chunk " + Arrays.toString(metadata.getPath()) +
        " " + metadata.getType() + " count=" + metadata.getValueCount());
    }
    
    List<Page> pagesInChunk = new ArrayList<Page>();
    long valuesCountReadSoFar = 0;
    while (valuesCountReadSoFar < metadata.getValueCount()) {
      PageHeader pageHeader = readNextDataPageHeader();
      pagesInChunk.add(
          new Page(
              BytesInput.copy(BytesInput.from(f, pageHeader.compressed_page_size)),
              pageHeader.data_page_header.num_values,
              pageHeader.uncompressed_page_size,
              parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding)
              ));
      valuesCountReadSoFar += pageHeader.data_page_header.num_values;
    }
    if (valuesCountReadSoFar != metadata.getValueCount()) {
      // Would be nice to have a CorruptParquetFileException or something as a subclass?
      throw new IOException(
          "Expected " + metadata.getValueCount() + " values in column chunk at " +
          filePath + " offset " + metadata.getFirstDataPageOffset() +
          " but got " + valuesCountReadSoFar + " values instead over " + pagesInChunk.size()
          + " pages ending at file offset " + f.getPos());
    }
    return pagesInChunk;
  }

  private PageHeader readNextDataPageHeader() throws IOException {
    PageHeader pageHeader;
    do {
      long pos = f.getPos();
      if (DEBUG) LOG.debug(pos + ": reading page");
      try {
        pageHeader = parquetMetadataConverter.readPageHeader(f);
        if (pageHeader.type != PageType.DATA_PAGE) {
          if (DEBUG) LOG.debug("not a data page, skipping " + pageHeader.compressed_page_size);
          f.skip(pageHeader.compressed_page_size);
        }
      } catch (IOException e) {
        throw new IOException("could not read page header at position " + pos, e);
      }
    } while (pageHeader.type != PageType.DATA_PAGE);
    return pageHeader;
  }

  public void close() throws IOException {
    f.close();
    this.codecFactory.release();
  }

}
