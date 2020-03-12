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
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Queue;

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
import org.apache.parquet.crypto.HiddenColumnException;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: should this actually be called RowGroupImpl or something?
 * The name is kind of confusing since it references three different "entities"
 * in our format: columns, chunks, and pages
 *
 */
class ColumnChunkPageReadStore implements PageReadStore, DictionaryPageReadStore {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkPageReadStore.class);

  /**
   * PageReader for a single column chunk. A column chunk contains
   * several pages, which are yielded one by one in order.
   *
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
    private int pageIndex = 0;
    
    private final BlockCipher.Decryptor blockDecryptor;
    private final boolean hiddenColumn;
    private final HiddenColumnException decryptionException;
    
    private final String[] columnPath;
    
    private byte[] dataPageAAD;
    private byte[] dictionaryPageAAD;

    ColumnChunkPageReader(BytesInputDecompressor decompressor, List<DataPage> compressedPages,
        DictionaryPage compressedDictionaryPage, OffsetIndex offsetIndex, long rowCount,
        String[] columnPath, BlockCipher.Decryptor blockDecryptor, byte[] fileAAD, 
        short rowGroupOrdinal, short columnOrdinal) {
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
      
      this.blockDecryptor = blockDecryptor;
      this.hiddenColumn = false;
      this.decryptionException = null;
      this.columnPath = columnPath;
 
      if (null != blockDecryptor) {
        dataPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, (short) 0);
        dictionaryPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DictionaryPage, rowGroupOrdinal, columnOrdinal, (short) -1);
      } 
    }
    
    // Creates hidden column object
    ColumnChunkPageReader(String[] columnPath, HiddenColumnException decryptionException) {
      this.decompressor = null;
      this.blockDecryptor = null;
      this.compressedPages = null;
      this.compressedDictionaryPage = null;
      this.valueCount = -1;
      this.hiddenColumn = true;
      this.decryptionException = decryptionException;
      this.columnPath = columnPath;
      this.offsetIndex = null;
      this.rowCount = 0;
    }
    
    private short getPageOrdinal(int currentPageIndex) {
      if (null == offsetIndex) {
        if (currentPageIndex > Short.MAX_VALUE) {
          throw new RuntimeException("Page ordinal exceeds limit " + currentPageIndex);
        }
        return (short)currentPageIndex;
      }
      
      return offsetIndex.getPageOrdinal(currentPageIndex);
    }

    @Override
    public long getTotalValueCount() {
      if (hiddenColumn) throw decryptionException;
      return valueCount;
    }

    @Override
    public DataPage readPage() {
      if (hiddenColumn) throw decryptionException;
      
      final DataPage compressedPage = compressedPages.poll();
      if (compressedPage == null) {
        // TODO ordinal??
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
            if (null != blockDecryptor) {
              bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
            }
            BytesInput decompressed = decompressor.decompress(bytes, dataPageV1.getUncompressedSize());
            
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
                  Math.toIntExact(offsetIndex.getLastRowIndex(currentPageIndex, rowCount) - firstRowIndex + 1),
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
        public DataPage visit(DataPageV2 dataPageV2) { // TODO simplify (one call to DataPageV2.uncompressed)
          if (!dataPageV2.isCompressed()) {
            if (offsetIndex == null) {
              if (null == blockDecryptor) {
                return dataPageV2;
              }
              else {
                BytesInput bytes = dataPageV2.getData();
                BytesInput decrypted;
                try {
                  decrypted = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
                } catch (IOException e) {
                  throw new ParquetDecodingException("could not decrypt page", e);
                }
                return DataPageV2.uncompressed(
                    dataPageV2.getRowCount(),
                    dataPageV2.getNullCount(),
                    dataPageV2.getValueCount(),
                    dataPageV2.getRepetitionLevels(),
                    dataPageV2.getDefinitionLevels(),
                    dataPageV2.getDataEncoding(),
                    decrypted,
                    dataPageV2.getStatistics());
              }
            } else {
              BytesInput bytes = dataPageV2.getData();
              if (null != blockDecryptor) {
                try {
                  bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
                } catch (IOException e) {
                  throw new ParquetDecodingException("could not decrypt page", e);
                }
              }
              return DataPageV2.uncompressed(
                  dataPageV2.getRowCount(),
                  dataPageV2.getNullCount(),
                  dataPageV2.getValueCount(),
                  offsetIndex.getFirstRowIndex(currentPageIndex),
                  dataPageV2.getRepetitionLevels(),
                  dataPageV2.getDefinitionLevels(),
                  dataPageV2.getDataEncoding(),
                  bytes,
                  dataPageV2.getStatistics());
            }
          }
          try {
            int uncompressedSize = Math.toIntExact(
                dataPageV2.getUncompressedSize()
                    - dataPageV2.getDefinitionLevels().size()
                    - dataPageV2.getRepetitionLevels().size());
            BytesInput bytes = dataPageV2.getData();
            if (null != blockDecryptor) {
              bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
            }
            BytesInput decompressed = decompressor.decompress(bytes, uncompressedSize);
            if (offsetIndex == null) {
              return DataPageV2.uncompressed(
                  dataPageV2.getRowCount(),
                  dataPageV2.getNullCount(),
                  dataPageV2.getValueCount(),
                  dataPageV2.getRepetitionLevels(),
                  dataPageV2.getDefinitionLevels(),
                  dataPageV2.getDataEncoding(),
                  decompressed,
                  dataPageV2.getStatistics());
            } else {
              return DataPageV2.uncompressed(
                  dataPageV2.getRowCount(),
                  dataPageV2.getNullCount(),
                  dataPageV2.getValueCount(),
                  offsetIndex.getFirstRowIndex(currentPageIndex),
                  dataPageV2.getRepetitionLevels(),
                  dataPageV2.getDefinitionLevels(),
                  dataPageV2.getDataEncoding(),
                  decompressed,
                  dataPageV2.getStatistics());
            }
          } catch (IOException e) {
            throw new ParquetDecodingException("could not decompress page", e);
          }
        }
      });
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (hiddenColumn) throw decryptionException;
      if (compressedDictionaryPage == null) {
        return null;
      }
      try {
        BytesInput bytes = compressedDictionaryPage.getBytes();
        if (null != blockDecryptor) {
          bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dictionaryPageAAD));
        }
        DictionaryPage decompressedPage = new DictionaryPage(
          decompressor.decompress(bytes, compressedDictionaryPage.getUncompressedSize()),
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
  }

  private final Map<ColumnDescriptor, ColumnChunkPageReader> readers = new HashMap<ColumnDescriptor, ColumnChunkPageReader>();
  private final long rowCount;
  private final RowRanges rowRanges;

  public ColumnChunkPageReadStore(long rowCount) {
    this.rowCount = rowCount;
    rowRanges = null;
  }

  ColumnChunkPageReadStore(RowRanges rowRanges) {
    this.rowRanges = rowRanges;
    rowCount = rowRanges.rowCount();
  }

  @Override
  public long getRowCount() {
    return rowCount;
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
      throw new RuntimeException(path+ " was added twice");
    }
  }

  void addHiddenColumn(ColumnDescriptor path, HiddenColumnException decryptionException) {
    addColumn(path, new ColumnChunkPageReader(path.getPath(), decryptionException));
  }
}
