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
package redelm.hadoop;

import static redelm.Log.DEBUG;
import static redelm.bytes.BytesUtils.readIntLittleEndian;
import static redelm.hadoop.RedelmFileWriter.MAGIC;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import redelm.Log;
import redelm.bytes.BytesInput;
import redelm.bytes.BytesUtils;
import redelm.hadoop.CodecFactory.BytesDecompressor;
import redelm.hadoop.metadata.BlockMetaData;
import redelm.hadoop.metadata.ColumnChunkMetaData;
import redelm.hadoop.metadata.RedelmMetaData;
import redelm.redfile.RedFileMetadataConverter;
import redfile.PageHeader;
import redfile.PageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.Utils;

/**
 * Reads a RedElm file
 *
 * @author Julien Le Dem
 *
 */
public class RedelmFileReader {
  private static final Log LOG = Log.getLog(RedelmFileReader.class);

  private static RedFileMetadataConverter redFileMetadataConverter = new RedFileMetadataConverter();

  private static RedelmMetaData deserializeFooter(InputStream is) throws IOException {
    redfile.FileMetaData redFileMetadata = redFileMetadataConverter.readFileMetaData(is);
    if (Log.DEBUG) LOG.debug(redFileMetadataConverter.toString(redFileMetadata));
    RedelmMetaData metadata = redFileMetadataConverter.fromRedFileMetadata(redFileMetadata);
    if (Log.DEBUG) LOG.debug(RedelmMetaData.toPrettyJSON(metadata));
    return metadata;
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
            RedelmMetaData redelmMetaData = RedelmFileReader.readFooter(configuration, currentFile);
            return new Footer(currentFile.getPath(), redelmMetaData);
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
        Path summaryPath = new Path(pathStatus.getPath(), "_RedElmSummary");
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
      RedelmMetaData redelmMetaData = redFileMetadataConverter.fromRedFileMetadata(redFileMetadataConverter.readFileMetaData(summary));
      result.add(new Footer(file, redelmMetaData));
    }
    summary.close();
    return result;
  }

  /**
   * Reads the meta data block in the footer of the file
   * @param configuration
   * @param file the RedElm File
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  public static final RedelmMetaData readFooter(Configuration configuration, Path file) throws IOException {
    FileSystem fileSystem = file.getFileSystem(configuration);
    return readFooter(configuration, fileSystem.getFileStatus(file));
  }

  /**
   * Reads the meta data block in the footer of the file
   * @param configuration
   * @param file the RedElm File
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  public static final RedelmMetaData readFooter(Configuration configuration, FileStatus file) throws IOException {
    FileSystem fileSystem = file.getPath().getFileSystem(configuration);
    FSDataInputStream f = fileSystem.open(file.getPath());
    long l = file.getLen();
    LOG.debug("File length " + l);
    int FOOTER_LENGTH_SIZE = 4;
    if (l <= MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException(file.getPath() + " is not a Red Elm file (too small)");
    }
    long footerLengthIndex = l - FOOTER_LENGTH_SIZE - MAGIC.length;
    LOG.debug("reading footer index at " + footerLengthIndex);

    f.seek(footerLengthIndex);
    int footerLength = readIntLittleEndian(f);
    byte[] magic = new byte[MAGIC.length];
    f.readFully(magic);
    if (!Arrays.equals(MAGIC, magic)) {
      throw new RuntimeException(file.getPath() + " is not a RedElm file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
    }
    long footerIndex = footerLengthIndex - footerLength;
    LOG.debug("read footer length: " + footerLength + ", footer index: " + footerIndex);
    if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
      throw new RuntimeException("corrupted file: the footer index is not within the file");
    }
    f.seek(footerIndex);
    return deserializeFooter(f);

  }
  private CodecFactory codecFactory;

  private final List<BlockMetaData> blocks;
  private final FSDataInputStream f;
  private int currentBlock = 0;
  private Set<String> paths = new HashSet<String>();
  private long previousReadIndex = 0;


  /**
   *
   * @param f the redelm file
   * @param blocks the blocks to read
   * @param colums the columns to read (their path)
   * @param codecClassName the codec used to compress the blocks
   * @throws IOException if the file can not be opened
   */
  public RedelmFileReader(Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<String[]> colums) throws IOException {
    FileSystem fs = FileSystem.get(configuration);
    this.f = fs.open(filePath);
    this.blocks = blocks;
    for (String[] path : colums) {
      paths.add(Arrays.toString(path));
    }
    this.codecFactory = new CodecFactory(configuration);
  }

  /**
   * reads all the columns requested in the next block
   * @return the block data for the next block
   * @throws IOException if an error occurs while reading
   * @return how many records where read or 0 if end reached.
   */
  public long readColumns(PageConsumer pageConsumer) throws IOException {
    if (currentBlock == blocks.size()) {
      return 0;
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    LOG.info("starting reading block #" + currentBlock);
    long t0 = System.currentTimeMillis();
    for (ColumnChunkMetaData mc : block.getColumns()) {
      String pathKey = Arrays.toString(mc.getPath());
      if (paths.contains(pathKey)) {
        f.seek(mc.getFirstDataPage());
        if (DEBUG) LOG.debug(f.getPos() + ": start column chunk " + Arrays.toString(mc.getPath()) + " " + mc.getType() + " count=" + mc.getValueCount());
        BytesDecompressor decompressor = codecFactory.getDecompressor(mc.getCodec());
        try {
          long valuesCountReadSoFar = 0;
          while (valuesCountReadSoFar < mc.getValueCount()) {
            PageHeader pageHeader = readNextDataPageHeader();
            pageConsumer.consumePage(
                mc.getPath(),
                pageHeader.data_page.num_values,
                decompressor.decompress(BytesInput.from(f, pageHeader.compressed_page_size), pageHeader.uncompressed_page_size));
            valuesCountReadSoFar += pageHeader.data_page.num_values;
          }
        } finally {
          decompressor.release();
        }
      }
    }
    long t1 = System.currentTimeMillis();
    LOG.info("block data read in " + (t1 - t0) + " ms");
    ++currentBlock;
    return block.getRowCount();
  }

  private PageHeader readNextDataPageHeader() throws IOException {
    PageHeader pageHeader;
    do {
      long pos = f.getPos();
      if (DEBUG) LOG.debug(pos + ": reading page");
      try {
        pageHeader = redFileMetadataConverter.readPageHeader(f);
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
  }

}
