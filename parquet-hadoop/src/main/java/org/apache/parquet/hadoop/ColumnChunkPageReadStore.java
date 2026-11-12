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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Queue;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.filter2.columnindex.RowRanges;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: should this actually be called RowGroupImpl or something?
 * The name is kind of confusing since it references three different "entities"
 * in our format: columns, chunks, and pages
 */
class ColumnChunkPageReadStore implements PageReadStore, DictionaryPageReadStore {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkPageReadStore.class);

  /**
   * PageReader for a single column chunk. A column chunk contains
   * several pages, which are yielded one by one in order.
   * <p>
   * This implementation is provided with a list of pages, each of which
   * is decompressed and passed through.
   */
  static final class ColumnChunkPageReader implements PageReader {

    private final BytesInputDecompressor decompressor;
    private final long valueCount;
    private final Queue<DataPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    // null means no page synchronization is required; firstRowIndex will not be returned by the pages
    private final OffsetIndex offsetIndex;
    private final long rowCount;
    private final ParquetReadOptions options;
    private int pageIndex = 0;

    private final BlockCipher.Decryptor blockDecryptor;
    private final byte[] dataPageAAD;
    private final byte[] dictionaryPageAAD;
    private final ByteBufferReleaser releaser;

    ColumnChunkPageReader(
        BytesInputDecompressor decompressor,
        List<DataPage> compressedPages,
        DictionaryPage compressedDictionaryPage,
        OffsetIndex offsetIndex,
        long rowCount,
        BlockCipher.Decryptor blockDecryptor,
        byte[] fileAAD,
        int rowGroupOrdinal,
        int columnOrdinal,
        ParquetReadOptions options) {
      this.decompressor = decompressor;
      this.compressedPages = new ArrayDeque<DataPage>(compressedPages);
      this.compressedDictionaryPage = compressedDictionaryPage;
      long count = 0;
      for (DataPage p : compressedPages) {
        count += p.getValueCount();
      }
      this.valueCount = count;
      this.offsetIndex = offsetIndex;
      this.rowCount = rowCount;
      this.options = options;
      this.releaser = new ByteBufferReleaser(options.getAllocator());
      this.blockDecryptor = blockDecryptor;
      if (null != blockDecryptor) {
        dataPageAAD =
            AesCipher.createModuleAAD(fileAAD, ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
        dictionaryPageAAD = AesCipher.createModuleAAD(
            fileAAD, ModuleType.DictionaryPage, rowGroupOrdinal, columnOrdinal, -1);
      } else {
        dataPageAAD = null;
        dictionaryPageAAD = null;
      }
    }

    private int getPageOrdinal(int currentPageIndex) {
      if (null == offsetIndex) {
        return currentPageIndex;
      }

      return offsetIndex.getPageOrdinal(currentPageIndex);
    }

    @Override
    public long getTotalValueCount() {
      return valueCount;
    }

    @Override
    public DataPage readPage() {
      final DataPage compressedPage = compressedPages.poll();
      if (compressedPage == null) {
        return null;
      }
      final int currentPageIndex = pageIndex++;

      if (null != blockDecryptor) {
        AesCipher.quickUpdatePageAAD(dataPageAAD, getPageOrdinal(currentPageIndex));
      }

      return compressedPage.accept(new DataPage.Visitor<DataPage>() {
        @Override
        public DataPage visit(DataPageV1 dataPageV1) {
          try {
            BytesInput bytes = dataPageV1.getBytes();
            BytesInput decompressed;

            if (options.getAllocator().isDirect() && options.useOffHeapDecryptBuffer()) {
              ByteBuffer byteBuffer = bytes.toByteBuffer(releaser);
              if (!byteBuffer.isDirect()) {
                throw new ParquetDecodingException("Expected a direct buffer");
              }
              if (blockDecryptor != null) {
                byteBuffer = blockDecryptor.decrypt(byteBuffer, dataPageAAD);
              }
              long compressedSize = byteBuffer.limit();

              ByteBuffer decompressedBuffer =
                  options.getAllocator().allocate(dataPageV1.getUncompressedSize());
              releaser.releaseLater(decompressedBuffer);
              long start = System.nanoTime();
              decompressor.decompress(
                  byteBuffer,
                  (int) compressedSize,
                  decompressedBuffer,
                  dataPageV1.getUncompressedSize());
              setDecompressMetrics(bytes, start);
              decompressedBuffer.flip();
              decompressed = BytesInput.from(decompressedBuffer);
            } else { // use on-heap buffer
              if (null != blockDecryptor) {
                bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
              }
              long start = System.nanoTime();
              decompressed =
                  BytesInput.copy(decompressor.decompress(bytes, dataPageV1.getUncompressedSize()));
              setDecompressMetrics(bytes, start);
            }

            final DataPageV1 decompressedPage;
            if (offsetIndex == null) {
              decompressedPage = new DataPageV1(
                  decompressed,
                  dataPageV1.getValueCount(),
                  dataPageV1.getUncompressedSize(),
                  dataPageV1.getStatistics(),
                  dataPageV1.getRlEncoding(),
                  dataPageV1.getDlEncoding(),
                  dataPageV1.getValueEncoding());
            } else {
              long firstRowIndex = offsetIndex.getFirstRowIndex(currentPageIndex);
              decompressedPage = new DataPageV1(
                  decompressed,
                  dataPageV1.getValueCount(),
                  dataPageV1.getUncompressedSize(),
                  firstRowIndex,
                  Math.toIntExact(offsetIndex.getLastRowIndex(currentPageIndex, rowCount)
                      - firstRowIndex
                      + 1),
                  dataPageV1.getStatistics(),
                  dataPageV1.getRlEncoding(),
                  dataPageV1.getDlEncoding(),
                  dataPageV1.getValueEncoding());
            }
            if (dataPageV1.getCrc().isPresent()) {
              decompressedPage.setCrc(dataPageV1.getCrc().getAsInt());
            }
            return decompressedPage;
          } catch (IOException e) {
            throw new ParquetDecodingException("could not decompress page", e);
          }
        }

        @Override
        public DataPage visit(DataPageV2 dataPageV2) {
          if (dataPageV2 instanceof FullBodyEncryptedDataPageV2) {
            return readPageV2WithEncryptedLevels(
                (FullBodyEncryptedDataPageV2) dataPageV2, currentPageIndex);
          }
          return readPageV2WithPlaintextLevels(dataPageV2, currentPageIndex);
        }
      });
    }

    /**
     * Reads a page whose levels are not encrypted. If the column is encrypted, only the values are an
     * encrypted module.
     *
     * <p>An encrypted page with plaintext levels does not follow the Parquet encryption spec, which
     * encrypts the levels and the values as one module. Such pages are written only by parquet-java
     * 1.18.0 and earlier, and are read here for backward compatibility only.
     */
    private DataPageV2 readPageV2WithPlaintextLevels(DataPageV2 page, int currentPageIndex) {
      if (!page.isCompressed() && offsetIndex == null && null == blockDecryptor) {
        return page;
      }
      boolean offHeap = decryptsOffHeap();
      BytesInput values = page.getData();
      try {
        if (null != blockDecryptor) {
          values = BytesInput.from(decryptDataPageModule(values, offHeap));
        } else if (offHeap) {
          values = BytesInput.from(toDirectBuffer(values));
        }
        if (page.isCompressed()) {
          int levelsByteLength =
              Math.toIntExact(page.getRepetitionLevels().size()
                  + page.getDefinitionLevels().size());
          values = decompressValues(values, valuesUncompressedSize(page, levelsByteLength), offHeap);
        }
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decompress page", e);
      }
      return uncompressedPageV2(
          page, page.getRepetitionLevels(), page.getDefinitionLevels(), values, currentPageIndex);
    }

    /**
     * Reads a page whose levels and values are one encrypted module: decrypts the body, splits it into
     * levels and values, and decompresses the values.
     */
    private DataPageV2 readPageV2WithEncryptedLevels(FullBodyEncryptedDataPageV2 page, int currentPageIndex) {
      boolean offHeap = decryptsOffHeap();
      int repetitionLevelsByteLength = page.getRepetitionLevelsByteLength();
      int definitionLevelsByteLength = page.getDefinitionLevelsByteLength();
      int levelsByteLength = repetitionLevelsByteLength + definitionLevelsByteLength;
      try {
        ByteBuffer body = decryptDataPageModule(page.getData(), offHeap);
        checkDecryptedPageSize(body.limit(), levelsByteLength);
        BytesInput repetitionLevels = BytesInput.from(body.slice(0, repetitionLevelsByteLength));
        BytesInput definitionLevels =
            BytesInput.from(body.slice(repetitionLevelsByteLength, definitionLevelsByteLength));
        BytesInput values = BytesInput.from(body.slice(levelsByteLength, body.limit() - levelsByteLength));
        if (page.isCompressed()) {
          values = decompressValues(values, valuesUncompressedSize(page, levelsByteLength), offHeap);
        }
        return uncompressedPageV2(page, repetitionLevels, definitionLevels, values, currentPageIndex);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decompress page", e);
      }
    }

    private void checkDecryptedPageSize(int decryptedSize, int levelsByteLength) {
      if (decryptedSize < levelsByteLength) {
        throw new ParquetDecodingException("Decrypted DataPageV2 body has " + decryptedSize
            + " bytes, which is less than the " + levelsByteLength
            + " bytes of repetition and definition levels in the page header");
      }
    }

    private static int valuesUncompressedSize(DataPageV2 page, int levelsByteLength) {
      return Math.toIntExact(page.getUncompressedSize() - levelsByteLength);
    }

    private boolean decryptsOffHeap() {
      return options.getAllocator().isDirect() && options.useOffHeapDecryptBuffer();
    }

    /** Decrypts in place if off heap, otherwise into a new heap buffer. */
    private ByteBuffer decryptDataPageModule(BytesInput module, boolean offHeap) throws IOException {
      if (offHeap) {
        return blockDecryptor.decrypt(toDirectBuffer(module), dataPageAAD);
      }
      return ByteBuffer.wrap(blockDecryptor.decrypt(module.toByteArray(), dataPageAAD));
    }

    private ByteBuffer toDirectBuffer(BytesInput bytes) {
      ByteBuffer buffer = bytes.toByteBuffer(releaser);
      if (!buffer.isDirect()) {
        throw new ParquetDecodingException("Expected a direct buffer");
      }
      return buffer;
    }

    private BytesInput decompressValues(BytesInput compressed, int uncompressedSize, boolean offHeap)
        throws IOException {
      long start = System.nanoTime();
      BytesInput decompressed;
      if (offHeap) {
        ByteBuffer input = toDirectBuffer(compressed);
        ByteBuffer output = options.getAllocator().allocate(uncompressedSize);
        releaser.releaseLater(output);
        decompressor.decompress(input, input.limit(), output, uncompressedSize);
        output.flip();
        decompressed = BytesInput.from(output);
      } else {
        decompressed = BytesInput.copy(decompressor.decompress(compressed, uncompressedSize));
      }
      setDecompressMetrics(compressed, start);
      return decompressed;
    }

    private DataPageV2 uncompressedPageV2(
        DataPageV2 source,
        BytesInput repetitionLevels,
        BytesInput definitionLevels,
        BytesInput values,
        int currentPageIndex) {
      final DataPageV2 decompressedPage;
      if (offsetIndex == null) {
        decompressedPage = DataPageV2.uncompressed(
            source.getRowCount(),
            source.getNullCount(),
            source.getValueCount(),
            repetitionLevels,
            definitionLevels,
            source.getDataEncoding(),
            values,
            source.getStatistics());
      } else {
        decompressedPage = DataPageV2.uncompressed(
            source.getRowCount(),
            source.getNullCount(),
            source.getValueCount(),
            offsetIndex.getFirstRowIndex(currentPageIndex),
            repetitionLevels,
            definitionLevels,
            source.getDataEncoding(),
            values,
            source.getStatistics());
      }
      if (source.getCrc().isPresent()) {
        decompressedPage.setCrc(source.getCrc().getAsInt());
      }
      return decompressedPage;
    }

    private void setDecompressMetrics(BytesInput bytes, long start) {
      final ParquetMetricsCallback metricsCallback = options.getMetricsCallback();
      if (metricsCallback != null) {
        long time = Math.max(System.nanoTime() - start, 0);
        long len = bytes.size();
        double throughput = ((double) len / time) * ((double) 1000_000_000L) / (1024 * 1024);
        LOG.debug(
            "Decompress block: Length: {} MB, Time: {} msecs, throughput: {} MB/s",
            len / (1024 * 1024),
            time / 1000_000L,
            throughput);
        metricsCallback.setDuration(ParquetFileReaderMetrics.DecompressTime.name(), time);
        metricsCallback.setValueLong(ParquetFileReaderMetrics.DecompressSize.name(), len);
        metricsCallback.setValueDouble(ParquetFileReaderMetrics.DecompressThroughput.name(), throughput);
      }
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (compressedDictionaryPage == null) {
        return null;
      }
      try {
        BytesInput bytes = compressedDictionaryPage.getBytes();
        if (null != blockDecryptor) {
          bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dictionaryPageAAD));
        }
        long start = System.nanoTime();
        setDecompressMetrics(bytes, start);
        DictionaryPage decompressedPage = new DictionaryPage(
            decompressor
                .decompress(bytes, compressedDictionaryPage.getUncompressedSize())
                .copy(releaser),
            compressedDictionaryPage.getDictionarySize(),
            compressedDictionaryPage.getEncoding());
        if (compressedDictionaryPage.getCrc().isPresent()) {
          decompressedPage.setCrc(compressedDictionaryPage.getCrc().getAsInt());
        }
        return decompressedPage;
      } catch (IOException e) {
        throw new ParquetDecodingException("Could not decompress dictionary page", e);
      }
    }

    @Override
    public void close() {
      releaser.close();
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageReader> readers =
      new HashMap<ColumnDescriptor, ColumnChunkPageReader>();
  private final long rowCount;
  private final long rowIndexOffset;
  private final RowRanges rowRanges;
  private ByteBufferAllocator allocator;
  private ByteBufferReleaser releaser;

  public ColumnChunkPageReadStore(long rowCount) {
    this(rowCount, -1);
  }

  ColumnChunkPageReadStore(RowRanges rowRanges) {
    this(rowRanges, -1);
  }

  ColumnChunkPageReadStore(long rowCount, long rowIndexOffset) {
    this.rowCount = rowCount;
    this.rowIndexOffset = rowIndexOffset;
    rowRanges = null;
  }

  ColumnChunkPageReadStore(RowRanges rowRanges, long rowIndexOffset) {
    this.rowRanges = rowRanges;
    this.rowIndexOffset = rowIndexOffset;
    rowCount = rowRanges.rowCount();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public Optional<Long> getRowIndexOffset() {
    return rowIndexOffset < 0 ? Optional.empty() : Optional.of(rowIndexOffset);
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor path) {
    final PageReader pageReader = readers.get(path);
    if (pageReader == null) {
      throw new IllegalArgumentException(path + " is not in the store: " + readers.keySet() + " " + rowCount);
    }
    return pageReader;
  }

  @Override
  public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
    return readers.get(descriptor).readDictionaryPage();
  }

  @Override
  public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
    return rowRanges == null ? Optional.empty() : Optional.of(rowRanges.iterator());
  }

  void addColumn(ColumnDescriptor path, ColumnChunkPageReader reader) {
    if (readers.put(path, reader) != null) {
      throw new RuntimeException(path + " was added twice");
    }
  }

  void setReleaser(ByteBufferReleaser releaser) {
    this.releaser = releaser;
  }

  @Override
  public void close() {
    // Wrap each reader + the releaser as an AutoCloseable so AutoCloseables.uncheckedClose()
    // releases every resource even if one fails, and aggregates failures via suppressed exceptions
    List<AutoCloseable> toClose = new ArrayList<>(readers.size() + 1);
    readers.values().forEach(reader -> toClose.add(reader));
    toClose.add(releaser);
    AutoCloseables.uncheckedClose(toClose);
  }
}
