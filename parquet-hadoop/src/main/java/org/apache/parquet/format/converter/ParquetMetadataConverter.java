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
package org.apache.parquet.format.converter;

import static org.apache.parquet.format.Util.readFileMetaData;
import static org.apache.parquet.format.Util.writePageHeader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.Log;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.column.statistics.ColumnStatisticsOpts;
import org.apache.parquet.column.statistics.StatisticsOpts;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOpts;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterStatistics;
import org.apache.parquet.format.BloomFilter;
import org.apache.parquet.format.BloomFilterStrategy;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.TypeVisitor;
import org.apache.parquet.schema.Types;

// TODO: This file has become too long!
// TODO: Lets split it up: https://issues.apache.org/jira/browse/PARQUET-310
public class ParquetMetadataConverter {

  public static final MetadataFilter NO_FILTER = new NoFilter();
  public static final MetadataFilter SKIP_ROW_GROUPS = new SkipMetadataFilter();
  public static final long MAX_STATS_SIZE = 4096; // limit stats to 4k

  private static final Log LOG = Log.getLog(ParquetMetadataConverter.class);

  // NOTE: this cache is for memory savings, not cpu savings, and is used to de-duplicate
  // sets of encodings. It is important that all collections inserted to this cache be
  // immutable and have thread-safe read-only access. This can be achieved by wrapping
  // an unsynchronized collection in Collections.unmodifiable*(), and making sure to not
  // keep any references to the original collection.
  private static final ConcurrentHashMap<Set<org.apache.parquet.column.Encoding>, Set<org.apache.parquet.column.Encoding>>
      cachedEncodingSets = new ConcurrentHashMap<Set<org.apache.parquet.column.Encoding>, Set<org.apache.parquet.column.Encoding>>();

  public FileMetaData toParquetMetadata(int currentVersion, ParquetMetadata parquetMetadata) {
    List<BlockMetaData> blocks = parquetMetadata.getBlocks();
    List<RowGroup> rowGroups = new ArrayList<RowGroup>();
    long numRows = 0;
    for (BlockMetaData block : blocks) {
      numRows += block.getRowCount();
      addRowGroup(parquetMetadata, rowGroups, block);
    }
    FileMetaData fileMetaData = new FileMetaData(
        currentVersion,
        toParquetSchema(parquetMetadata.getFileMetaData().getSchema()),
        numRows,
        rowGroups);

    Set<Entry<String, String>> keyValues = parquetMetadata.getFileMetaData().getKeyValueMetaData().entrySet();
    for (Entry<String, String> keyValue : keyValues) {
      addKeyValue(fileMetaData, keyValue.getKey(), keyValue.getValue());
    }

    fileMetaData.setCreated_by(parquetMetadata.getFileMetaData().getCreatedBy());
    return fileMetaData;
  }

  // Visible for testing
  List<SchemaElement> toParquetSchema(MessageType schema) {
    List<SchemaElement> result = new ArrayList<SchemaElement>();
    addToList(result, schema);
    return result;
  }

  private void addToList(final List<SchemaElement> result, org.apache.parquet.schema.Type field) {
    field.accept(new TypeVisitor() {
      @Override
      public void visit(PrimitiveType primitiveType) {
        SchemaElement element = new SchemaElement(primitiveType.getName());
        element.setRepetition_type(toParquetRepetition(primitiveType.getRepetition()));
        element.setType(getType(primitiveType.getPrimitiveTypeName()));
        if (primitiveType.getOriginalType() != null) {
          element.setConverted_type(getConvertedType(primitiveType.getOriginalType()));
        }
        if (primitiveType.getDecimalMetadata() != null) {
          element.setPrecision(primitiveType.getDecimalMetadata().getPrecision());
          element.setScale(primitiveType.getDecimalMetadata().getScale());
        }
        if (primitiveType.getTypeLength() > 0) {
          element.setType_length(primitiveType.getTypeLength());
        }
        result.add(element);
      }

      @Override
      public void visit(MessageType messageType) {
        SchemaElement element = new SchemaElement(messageType.getName());
        visitChildren(result, messageType.asGroupType(), element);
      }

      @Override
      public void visit(GroupType groupType) {
        SchemaElement element = new SchemaElement(groupType.getName());
        element.setRepetition_type(toParquetRepetition(groupType.getRepetition()));
        if (groupType.getOriginalType() != null) {
          element.setConverted_type(getConvertedType(groupType.getOriginalType()));
        }
        visitChildren(result, groupType, element);
      }

      private void visitChildren(
        final List<SchemaElement> result,
        GroupType groupType,
        SchemaElement element) {
        element.setNum_children(groupType.getFieldCount());
        result.add(element);
        for (org.apache.parquet.schema.Type field : groupType.getFields()) {
          addToList(result, field);
        }
      }
    });
  }

  private void addRowGroup(ParquetMetadata parquetMetadata, List<RowGroup> rowGroups, BlockMetaData block) {
    //rowGroup.total_byte_size = ;
    List<ColumnChunkMetaData> columns = block.getColumns();
    List<ColumnChunk> parquetColumns = new ArrayList<ColumnChunk>();
    for (ColumnChunkMetaData columnMetaData : columns) {
      ColumnChunk columnChunk = new ColumnChunk(columnMetaData.getFirstDataPageOffset()); // verify this is the right offset
      columnChunk.file_path = block.getPath(); // they are in the same file for now
      columnChunk.meta_data = new ColumnMetaData(
          getType(columnMetaData.getType()),
          toFormatEncodings(columnMetaData.getEncodings()),
          Arrays.asList(columnMetaData.getPath().toArray()),
          columnMetaData.getCodec().getParquetCompressionCodec(),
          columnMetaData.getValueCount(),
          columnMetaData.getTotalUncompressedSize(),
          columnMetaData.getTotalSize(),
          columnMetaData.getFirstDataPageOffset());
      columnChunk.meta_data.dictionary_page_offset = columnMetaData.getDictionaryPageOffset();
      if (!columnMetaData.getStatistics().isEmpty()) {
        columnChunk.meta_data.setStatistics(toParquetStatistics(columnMetaData.getStatistics()));
      }
      if (columnMetaData.getEncodingStats() != null) {
        columnChunk.meta_data.setEncoding_stats(convertEncodingStats(columnMetaData.getEncodingStats()));
      }
//      columnChunk.meta_data.index_page_offset = ;
//      columnChunk.meta_data.key_value_metadata = ; // nothing yet

      parquetColumns.add(columnChunk);
    }
    RowGroup rowGroup = new RowGroup(parquetColumns, block.getTotalByteSize(), block.getRowCount());
    rowGroups.add(rowGroup);
  }

  private List<Encoding> toFormatEncodings(Set<org.apache.parquet.column.Encoding> encodings) {
    List<Encoding> converted = new ArrayList<Encoding>(encodings.size());
    for (org.apache.parquet.column.Encoding encoding : encodings) {
      converted.add(getEncoding(encoding));
    }
    return converted;
  }

  // Visible for testing
  Set<org.apache.parquet.column.Encoding> fromFormatEncodings(List<Encoding> encodings) {
    Set<org.apache.parquet.column.Encoding> converted = new HashSet<org.apache.parquet.column.Encoding>();

    for (Encoding encoding : encodings) {
      converted.add(getEncoding(encoding));
    }

    // make converted unmodifiable, drop reference to modifiable copy
    converted = Collections.unmodifiableSet(converted);

    // atomically update the cache
    Set<org.apache.parquet.column.Encoding> cached = cachedEncodingSets.putIfAbsent(converted, converted);

    if (cached == null) {
      // cached == null signifies that converted was *not* in the cache previously
      // so we can return converted instead of throwing it away, it has now
      // been cached
      cached = converted;
    }

    return cached;
  }

  public org.apache.parquet.column.Encoding getEncoding(Encoding encoding) {
    return org.apache.parquet.column.Encoding.valueOf(encoding.name());
  }

  public Encoding getEncoding(org.apache.parquet.column.Encoding encoding) {
    return Encoding.valueOf(encoding.name());
  }

  public EncodingStats convertEncodingStats(List<PageEncodingStats> stats) {
    if (stats == null) {
      return null;
    }

    EncodingStats.Builder builder = new EncodingStats.Builder();
    for (PageEncodingStats stat : stats) {
      switch (stat.getPage_type()) {
        case DATA_PAGE_V2:
          builder.withV2Pages();
          // falls through
        case DATA_PAGE:
          builder.addDataEncoding(
              getEncoding(stat.getEncoding()), stat.getCount());
          break;
        case DICTIONARY_PAGE:
          builder.addDictEncoding(
              getEncoding(stat.getEncoding()), stat.getCount());
          break;
      }
    }
    return builder.build();
  }

  public List<PageEncodingStats> convertEncodingStats(EncodingStats stats) {
    if (stats == null) {
      return null;
    }

    List<PageEncodingStats> formatStats = new ArrayList<PageEncodingStats>();
    for (org.apache.parquet.column.Encoding encoding : stats.getDictionaryEncodings()) {
      formatStats.add(new PageEncodingStats(
          PageType.DICTIONARY_PAGE, getEncoding(encoding),
          stats.getNumDictionaryPagesEncodedAs(encoding)));
    }
    PageType dataPageType = (stats.usesV2Pages() ? PageType.DATA_PAGE_V2 : PageType.DATA_PAGE);
    for (org.apache.parquet.column.Encoding encoding : stats.getDataEncodings()) {
      formatStats.add(new PageEncodingStats(
          dataPageType, getEncoding(encoding),
          stats.getNumDataPagesEncodedAs(encoding)));
    }
    return formatStats;
  }

  public static Statistics toParquetStatisticsWithoutBloomFilter(
      org.apache.parquet.column.statistics.Statistics statistics) {
    Statistics stats = new Statistics();
    // Don't write stats larger than the max size rather than truncating. The
    // rationale is that some engines may use the minimum value in the page as
    // the true minimum for aggregations and there is no way to mark that a
    // value has been truncated and is a lower bound and not in the page.
    if (!statistics.isEmpty() && statistics.isSmallerThan(MAX_STATS_SIZE)) {
      stats.setNull_count(statistics.getNumNulls());
      if (statistics.hasNonNullValue()) {
        stats.setMax(statistics.getMaxBytes());
        stats.setMin(statistics.getMinBytes());
      }
    }
    return stats;
  }

  /**
   * @deprecated Replaced by {@link #fromParquetStatistics(
   * String createdBy, Statistics statistics, PrimitiveTypeName type)}
   */
  @Deprecated
  public static org.apache.parquet.column.statistics.Statistics fromParquetStatistics(Statistics statistics, PrimitiveTypeName type) {
    return fromParquetStatistics(null, statistics, type);
  }

  public static Statistics toParquetStatistics(
      org.apache.parquet.column.statistics.Statistics statistics) {
    Statistics stats = new Statistics();
    if (!statistics.isEmpty()) {
      stats.setNull_count(statistics.getNumNulls());
      if (statistics.hasNonNullValue()) {
        stats.setMax(statistics.getMaxBytes());
        stats.setMin(statistics.getMinBytes());
        setBloomFilter(stats, statistics);
     }
    }
    return stats;
  }

  public static ByteBuffer serializeLongArray(long[] list) {
    ObjectOutputStream out = null;
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      out = new ObjectOutputStream(bos);
      out.writeObject(list);
      // Get the bytes of the serialized object
      byte[] buf = bos.toByteArray();
      return ByteBuffer.wrap(buf);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return ByteBuffer.allocate(0);
  }

  public static long[] deserializeLongArray(ByteBuffer buffer) {
    ObjectInputStream in = null;
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(buffer.array());
      in = new ObjectInputStream(bis);
      long[] res = (long[]) in.readObject();
      return res;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

  private static void setBloomFilter(
      Statistics stats,
      org.apache.parquet.column.statistics.Statistics statistics) {
    if (!(statistics instanceof BloomFilterStatistics) || !((BloomFilterStatistics) statistics)
        .isBloomFilterEnabled()) {
      stats.setBloom_filter(null);
    } else {
      BloomFilterStatistics bfStatistics = (BloomFilterStatistics) statistics;
      BloomFilter bfStats =
          new BloomFilter(serializeLongArray(bfStatistics.getBloomFilter().getBitSet()),
              bfStatistics.getBloomFilter().getNumBits(),
              bfStatistics.getBloomFilter().getNumHashFunctions(),
              BloomFilterStrategy.MURMUR128_32);
      stats.setBloom_filter(bfStats);
    }
  }

  public static org.apache.parquet.column.statistics.Statistics fromParquetStatistics(
      String createdBy,
      Statistics statistics,
      PrimitiveTypeName type,
      boolean includeBF) {
    if (includeBF) {
      return fromParquetStatisticsWithBF(createdBy, statistics, type);
    } else {
      return fromParquetStatistics(createdBy, statistics, type);
    }
  }

  public static org.apache.parquet.column.statistics.Statistics fromParquetStatistics(
      String createdBy,
      Statistics statistics,
      PrimitiveTypeName type) {
    org.apache.parquet.column.statistics.Statistics stats;
    // If there was no statistics written to the footer, create an empty Statistics object and return
    if (statistics != null && !CorruptStatistics.shouldIgnoreStatistics(createdBy, type)) {
      // create stats object based on the column type
      stats = org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(type, null);

      if (statistics.isSetMax() && statistics.isSetMin()) {
        stats.setMinMaxFromBytes(statistics.min.array(), statistics.max.array());
      }
      stats.setNumNulls(statistics.null_count);
    } else {
      stats = org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(type, null);
    }
    return stats;
  }

  public static org.apache.parquet.column.statistics.Statistics fromParquetStatisticsWithBF(
      String createdBy,
      Statistics statistics,
      PrimitiveTypeName type) {
    org.apache.parquet.column.statistics.Statistics stats;
    // If there was no statistics written to the footer, create an empty Statistics object and return
    if (statistics != null && !CorruptStatistics.shouldIgnoreStatistics(createdBy, type)) {
      BloomFilterOpts.BloomFilterEntry opts = null;
      if (statistics.getBloom_filter() != null) {
        opts = new BloomFilterOpts.BloomFilterEntry(statistics.getBloom_filter().getNumBits(),
            statistics.getBloom_filter().getNumHashFunctions());
      }
      // create stats object based on the column type
      stats =
          org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(type,
              new ColumnStatisticsOpts(opts));

      if (statistics.isSetMax() && statistics.isSetMin()) {
        stats.setMinMaxFromBytes(statistics.min.array(), statistics.max.array());
      }

      // update data for bloom filter statistics
      if (statistics.getBloom_filter()!=null && stats instanceof BloomFilterStatistics) {
        ((BloomFilterStatistics) stats).getBloomFilter().setBitSet(
            deserializeLongArray(ByteBuffer.wrap(statistics.getBloom_filter().getBitSet())));
      }
      stats.setNumNulls(statistics.null_count);
    } else {
      stats = org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(type, null);
    }
    return stats;
  }

  public PrimitiveTypeName getPrimitive(Type type) {
    switch (type) {
      case BYTE_ARRAY: // TODO: rename BINARY and remove this switch
        return PrimitiveTypeName.BINARY;
      case INT64:
        return PrimitiveTypeName.INT64;
      case INT32:
        return PrimitiveTypeName.INT32;
      case BOOLEAN:
        return PrimitiveTypeName.BOOLEAN;
      case FLOAT:
        return PrimitiveTypeName.FLOAT;
      case DOUBLE:
        return PrimitiveTypeName.DOUBLE;
      case INT96:
        return PrimitiveTypeName.INT96;
      case FIXED_LEN_BYTE_ARRAY:
        return PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
      default:
        throw new RuntimeException("Unknown type " + type);
    }
  }

  // Visible for testing
  Type getType(PrimitiveTypeName type) {
    switch (type) {
      case INT64:
        return Type.INT64;
      case INT32:
        return Type.INT32;
      case BOOLEAN:
        return Type.BOOLEAN;
      case BINARY:
        return Type.BYTE_ARRAY;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case INT96:
        return Type.INT96;
      case FIXED_LEN_BYTE_ARRAY:
        return Type.FIXED_LEN_BYTE_ARRAY;
      default:
        throw new RuntimeException("Unknown primitive type " + type);
    }
  }

  // Visible for testing
  OriginalType getOriginalType(ConvertedType type) {
    switch (type) {
      case UTF8:
        return OriginalType.UTF8;
      case MAP:
        return OriginalType.MAP;
      case MAP_KEY_VALUE:
        return OriginalType.MAP_KEY_VALUE;
      case LIST:
        return OriginalType.LIST;
      case ENUM:
        return OriginalType.ENUM;
      case DECIMAL:
        return OriginalType.DECIMAL;
      case DATE:
        return OriginalType.DATE;
      case TIME_MILLIS:
        return OriginalType.TIME_MILLIS;
      case TIME_MICROS:
        return OriginalType.TIME_MICROS;
      case TIMESTAMP_MILLIS:
        return OriginalType.TIMESTAMP_MILLIS;
      case TIMESTAMP_MICROS:
        return OriginalType.TIMESTAMP_MICROS;
      case INTERVAL:
        return OriginalType.INTERVAL;
      case INT_8:
        return OriginalType.INT_8;
      case INT_16:
        return OriginalType.INT_16;
      case INT_32:
        return OriginalType.INT_32;
      case INT_64:
        return OriginalType.INT_64;
      case UINT_8:
        return OriginalType.UINT_8;
      case UINT_16:
        return OriginalType.UINT_16;
      case UINT_32:
        return OriginalType.UINT_32;
      case UINT_64:
        return OriginalType.UINT_64;
      case JSON:
        return OriginalType.JSON;
      case BSON:
        return OriginalType.BSON;
      default:
        throw new RuntimeException("Unknown converted type " + type);
    }
  }

  // Visible for testing
  ConvertedType getConvertedType(OriginalType type) {
    switch (type) {
      case UTF8:
        return ConvertedType.UTF8;
      case MAP:
        return ConvertedType.MAP;
      case MAP_KEY_VALUE:
        return ConvertedType.MAP_KEY_VALUE;
      case LIST:
        return ConvertedType.LIST;
      case ENUM:
        return ConvertedType.ENUM;
      case DECIMAL:
        return ConvertedType.DECIMAL;
      case DATE:
        return ConvertedType.DATE;
      case TIME_MILLIS:
        return ConvertedType.TIME_MILLIS;
      case TIME_MICROS:
        return ConvertedType.TIME_MICROS;
      case TIMESTAMP_MILLIS:
        return ConvertedType.TIMESTAMP_MILLIS;
      case TIMESTAMP_MICROS:
        return ConvertedType.TIMESTAMP_MICROS;
      case INTERVAL:
        return ConvertedType.INTERVAL;
      case INT_8:
        return ConvertedType.INT_8;
      case INT_16:
        return ConvertedType.INT_16;
      case INT_32:
        return ConvertedType.INT_32;
      case INT_64:
        return ConvertedType.INT_64;
      case UINT_8:
        return ConvertedType.UINT_8;
      case UINT_16:
        return ConvertedType.UINT_16;
      case UINT_32:
        return ConvertedType.UINT_32;
      case UINT_64:
        return ConvertedType.UINT_64;
      case JSON:
        return ConvertedType.JSON;
      case BSON:
        return ConvertedType.BSON;
      default:
        throw new RuntimeException("Unknown original type " + type);
     }
   }

  private static void addKeyValue(FileMetaData fileMetaData, String key, String value) {
    KeyValue keyValue = new KeyValue(key);
    keyValue.value = value;
    fileMetaData.addToKey_value_metadata(keyValue);
  }

  private static interface MetadataFilterVisitor<T, E extends Throwable> {
    T visit(NoFilter filter) throws E;
    T visit(SkipMetadataFilter filter) throws E;
    T visit(RangeMetadataFilter filter) throws E;
    T visit(OffsetMetadataFilter filter) throws E;
  }

  public abstract static class MetadataFilter {
    private MetadataFilter() {}
    abstract <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E;
  }

  /**
   * [ startOffset, endOffset )
   * @param startOffset
   * @param endOffset
   * @return the filter
   */
  public static MetadataFilter range(long startOffset, long endOffset) {
    return new RangeMetadataFilter(startOffset, endOffset);
  }

  public static MetadataFilter offsets(long... offsets) {
    Set<Long> set = new HashSet<Long>();
    for (long offset : offsets) {
      set.add(offset);
    }
    return new OffsetMetadataFilter(set);
  }

  private static final class NoFilter extends MetadataFilter {
    private NoFilter() {}
    @Override
    <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E {
      return visitor.visit(this);
    }
    @Override
    public String toString() {
      return "NO_FILTER";
    }
  }
  private static final class SkipMetadataFilter extends MetadataFilter {
    private SkipMetadataFilter() {}
    @Override
    <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E {
      return visitor.visit(this);
    }
    @Override
    public String toString() {
      return "SKIP_ROW_GROUPS";
    }
  }

  /**
   * [ startOffset, endOffset )
   * @author Julien Le Dem
   */
  // Visible for testing
  static final class RangeMetadataFilter extends MetadataFilter {
    final long startOffset;
    final long endOffset;

    RangeMetadataFilter(long startOffset, long endOffset) {
      super();
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    @Override
    <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E {
      return visitor.visit(this);
    }

    public boolean contains(long offset) {
      return offset >= this.startOffset && offset < this.endOffset;
    }

    @Override
    public String toString() {
      return "range(s:" + startOffset + ", e:" + endOffset + ")";
    }
  }

  static final class OffsetMetadataFilter extends MetadataFilter {
    private final Set<Long> offsets;

    public OffsetMetadataFilter(Set<Long> offsets) {
      this.offsets = offsets;
    }

    public boolean contains(long offset) {
      return offsets.contains(offset);
    }

    @Override
    <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Deprecated
  public ParquetMetadata readParquetMetadata(InputStream from) throws IOException {
    return readParquetMetadata(from, NO_FILTER, false);
  }

  // Visible for testing
  static FileMetaData filterFileMetaDataByMidpoint(FileMetaData metaData, RangeMetadataFilter filter) {
    List<RowGroup> rowGroups = metaData.getRow_groups();
    List<RowGroup> newRowGroups = new ArrayList<RowGroup>();
    for (RowGroup rowGroup : rowGroups) {
      long totalSize = 0;
      long startIndex = getOffset(rowGroup.getColumns().get(0));
      for (ColumnChunk col : rowGroup.getColumns()) {
        totalSize += col.getMeta_data().getTotal_compressed_size();
      }
      long midPoint = startIndex + totalSize / 2;
      if (filter.contains(midPoint)) {
        newRowGroups.add(rowGroup);
      }
    }
    metaData.setRow_groups(newRowGroups);
    return metaData;
  }

  // Visible for testing
  static FileMetaData filterFileMetaDataByStart(FileMetaData metaData, OffsetMetadataFilter filter) {
    List<RowGroup> rowGroups = metaData.getRow_groups();
    List<RowGroup> newRowGroups = new ArrayList<RowGroup>();
    for (RowGroup rowGroup : rowGroups) {
      long startIndex = getOffset(rowGroup.getColumns().get(0));
      if (filter.contains(startIndex)) {
        newRowGroups.add(rowGroup);
      }
    }
    metaData.setRow_groups(newRowGroups);
    return metaData;
  }

  static long getOffset(RowGroup rowGroup) {
    return getOffset(rowGroup.getColumns().get(0));
  }
  // Visible for testing
  static long getOffset(ColumnChunk columnChunk) {
    ColumnMetaData md = columnChunk.getMeta_data();
    long offset = md.getData_page_offset();
    if (md.isSetDictionary_page_offset() && offset > md.getDictionary_page_offset()) {
      offset = md.getDictionary_page_offset();
    }
    return offset;
  }

  public ParquetMetadata readParquetMetadata(
    final InputStream from,
    MetadataFilter filter) throws IOException{
    return readParquetMetadata(from, filter);
  }

  public ParquetMetadata readParquetMetadata(
      final InputStream from,
      MetadataFilter filter,
      boolean includeBF) throws IOException {
    FileMetaData fileMetaData = filter.accept(
      new MetadataFilterVisitor<FileMetaData, IOException>() {
        @Override
        public FileMetaData visit(NoFilter filter) throws IOException {
          return readFileMetaData(from);
        }

        @Override
        public FileMetaData visit(SkipMetadataFilter filter) throws IOException {
          return readFileMetaData(from, true);
        }

        @Override
        public FileMetaData visit(OffsetMetadataFilter filter) throws IOException {
          return filterFileMetaDataByStart(readFileMetaData(from), filter);
        }

        @Override
        public FileMetaData visit(RangeMetadataFilter filter) throws IOException {
          return filterFileMetaDataByMidpoint(readFileMetaData(from), filter);
        }
      });
    if (Log.DEBUG) LOG.debug(fileMetaData);
    ParquetMetadata parquetMetadata = fromParquetMetadata(fileMetaData, includeBF);
    if (Log.DEBUG) LOG.debug(ParquetMetadata.toPrettyJSON(parquetMetadata));
    return parquetMetadata;
  }

  public ParquetMetadata fromParquetMetadata(FileMetaData parquetMetadata) throws IOException {
    return fromParquetMetadata(parquetMetadata, false);
  }

  public ParquetMetadata fromParquetMetadata(
      FileMetaData parquetMetadata,
      boolean includeBF) throws IOException {
    MessageType messageType = fromParquetSchema(parquetMetadata.getSchema());
    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
    List<RowGroup> row_groups = parquetMetadata.getRow_groups();
    if (row_groups != null) {
      for (RowGroup rowGroup : row_groups) {
        BlockMetaData blockMetaData = new BlockMetaData();
        blockMetaData.setRowCount(rowGroup.getNum_rows());
        blockMetaData.setTotalByteSize(rowGroup.getTotal_byte_size());
        List<ColumnChunk> columns = rowGroup.getColumns();
        String filePath = columns.get(0).getFile_path();
        for (ColumnChunk columnChunk : columns) {
          if ((filePath == null && columnChunk.getFile_path() != null)
              || (filePath != null && !filePath.equals(columnChunk.getFile_path()))) {
            throw new ParquetDecodingException("all column chunks of the same row group must be in the same file for now");
          }
          ColumnMetaData metaData = columnChunk.meta_data;
          ColumnPath path = getPath(metaData);
          ColumnChunkMetaData column = ColumnChunkMetaData.get(
              path,
              messageType.getType(path.toArray()).asPrimitiveType().getPrimitiveTypeName(),
              CompressionCodecName.fromParquet(metaData.codec),
              convertEncodingStats(metaData.getEncoding_stats()),
              fromFormatEncodings(metaData.encodings),
              fromParquetStatistics(
                  parquetMetadata.created_by,
                  metaData.statistics,
                  messageType.getType(path.toArray()).asPrimitiveType().getPrimitiveTypeName(),
                  includeBF),
              metaData.data_page_offset,
              metaData.dictionary_page_offset,
              metaData.num_values,
              metaData.total_compressed_size,
              metaData.total_uncompressed_size);
          // TODO
          // index_page_offset
          // key_value_metadata
          blockMetaData.addColumn(column);
        }
        blockMetaData.setPath(filePath);
        blocks.add(blockMetaData);
      }
    }
    Map<String, String> keyValueMetaData = new HashMap<String, String>();
    List<KeyValue> key_value_metadata = parquetMetadata.getKey_value_metadata();
    if (key_value_metadata != null) {
      for (KeyValue keyValue : key_value_metadata) {
        keyValueMetaData.put(keyValue.key, keyValue.value);
      }
    }
    return new ParquetMetadata(
        new org.apache.parquet.hadoop.metadata.FileMetaData(messageType, keyValueMetaData, parquetMetadata.getCreated_by()),
        blocks);
  }

  private static ColumnPath getPath(ColumnMetaData metaData) {
    String[] path = metaData.path_in_schema.toArray(new String[metaData.path_in_schema.size()]);
    return ColumnPath.get(path);
  }

  // Visible for testing
  MessageType fromParquetSchema(List<SchemaElement> schema) {
    Iterator<SchemaElement> iterator = schema.iterator();
    SchemaElement root = iterator.next();
    Types.MessageTypeBuilder builder = Types.buildMessage();
    buildChildren(builder, iterator, root.getNum_children());
    return builder.named(root.name);
  }

  private void buildChildren(Types.GroupBuilder builder,
                             Iterator<SchemaElement> schema,
                             int childrenCount) {
    for (int i = 0; i < childrenCount; i++) {
      SchemaElement schemaElement = schema.next();

      // Create Parquet Type.
      Types.Builder childBuilder;
      if (schemaElement.type != null) {
        Types.PrimitiveBuilder primitiveBuilder = builder.primitive(
            getPrimitive(schemaElement.type),
            fromParquetRepetition(schemaElement.repetition_type));
        if (schemaElement.isSetType_length()) {
          primitiveBuilder.length(schemaElement.type_length);
        }
        if (schemaElement.isSetPrecision()) {
          primitiveBuilder.precision(schemaElement.precision);
        }
        if (schemaElement.isSetScale()) {
          primitiveBuilder.scale(schemaElement.scale);
        }
        childBuilder = primitiveBuilder;

      } else {
        childBuilder = builder.group(fromParquetRepetition(schemaElement.repetition_type));
        buildChildren((Types.GroupBuilder) childBuilder, schema, schemaElement.num_children);
      }

      if (schemaElement.isSetConverted_type()) {
        childBuilder.as(getOriginalType(schemaElement.converted_type));
      }
      if (schemaElement.isSetField_id()) {
        childBuilder.id(schemaElement.field_id);
      }

      childBuilder.named(schemaElement.name);
    }
  }

  // Visible for testing
  FieldRepetitionType toParquetRepetition(Repetition repetition) {
    return FieldRepetitionType.valueOf(repetition.name());
  }

  // Visible for testing
  Repetition fromParquetRepetition(FieldRepetitionType repetition) {
    return Repetition.valueOf(repetition.name());
  }

  @Deprecated
  public void writeDataPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      OutputStream to) throws IOException {
    writePageHeader(newDataPageHeader(uncompressedSize,
                                      compressedSize,
                                      valueCount,
                                      new org.apache.parquet.column.statistics.BooleanStatistics(),
                                      rlEncoding,
                                      dlEncoding,
                                      valuesEncoding), to);
  }

  public void writeDataPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.statistics.Statistics statistics,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      OutputStream to) throws IOException {
    writePageHeader(
        newDataPageHeader(uncompressedSize, compressedSize, valueCount, statistics,
            rlEncoding, dlEncoding, valuesEncoding),
        to);
  }

  private PageHeader newDataPageHeader(
      int uncompressedSize, int compressedSize,
      int valueCount,
      org.apache.parquet.column.statistics.Statistics statistics,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding) {
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);
    // TODO: pageHeader.crc = ...;
    pageHeader.setData_page_header(new DataPageHeader(
        valueCount,
        getEncoding(valuesEncoding),
        getEncoding(dlEncoding),
        getEncoding(rlEncoding)));
    if (!statistics.isEmpty()) {
      pageHeader.getData_page_header()
          .setStatistics(toParquetStatisticsWithoutBloomFilter(statistics));
    }
    return pageHeader;
  }

  public void writeDataPageV2Header(
      int uncompressedSize, int compressedSize,
      int valueCount, int nullCount, int rowCount,
      org.apache.parquet.column.statistics.Statistics statistics,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength, int dlByteLength,
      OutputStream to) throws IOException {
    writePageHeader(
        newDataPageV2Header(
            uncompressedSize, compressedSize,
            valueCount, nullCount, rowCount,
            statistics,
            dataEncoding,
            rlByteLength, dlByteLength), to);
  }

  private PageHeader newDataPageV2Header(
      int uncompressedSize, int compressedSize,
      int valueCount, int nullCount, int rowCount,
      org.apache.parquet.column.statistics.Statistics<?> statistics,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength, int dlByteLength) {
    // TODO: pageHeader.crc = ...;
    DataPageHeaderV2 dataPageHeaderV2 = new DataPageHeaderV2(
        valueCount, nullCount, rowCount,
        getEncoding(dataEncoding),
        dlByteLength, rlByteLength);
    if (!statistics.isEmpty()) {
      dataPageHeaderV2.setStatistics(toParquetStatisticsWithoutBloomFilter(statistics));
    }
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE_V2, uncompressedSize, compressedSize);
    pageHeader.setData_page_header_v2(dataPageHeaderV2);
    return pageHeader;
  }

  public void writeDictionaryPageHeader(
      int uncompressedSize, int compressedSize, int valueCount,
      org.apache.parquet.column.Encoding valuesEncoding, OutputStream to) throws IOException {
    PageHeader pageHeader = new PageHeader(PageType.DICTIONARY_PAGE, uncompressedSize, compressedSize);
    pageHeader.setDictionary_page_header(new DictionaryPageHeader(valueCount, getEncoding(valuesEncoding)));
    writePageHeader(pageHeader, to);
  }

}
