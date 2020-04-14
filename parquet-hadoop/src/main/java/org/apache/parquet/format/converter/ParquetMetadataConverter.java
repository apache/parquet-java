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

import static java.util.Optional.empty;

import static java.util.Optional.of;
import static org.apache.parquet.format.Util.readFileMetaData;
import static org.apache.parquet.format.Util.writePageHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.BloomFilterAlgorithm;
import org.apache.parquet.format.BloomFilterCompression;
import org.apache.parquet.format.BloomFilterHash;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.BsonType;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DateType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.EnumType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.JsonType;
import org.apache.parquet.format.ListType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.MilliSeconds;
import org.apache.parquet.format.NanoSeconds;
import org.apache.parquet.format.NullType;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.SplitBlockAlgorithm;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.Uncompressed;
import org.apache.parquet.format.XxHash;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.BinaryTruncator;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder.ColumnOrderName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.TypeVisitor;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: This file has become too long!
// TODO: Lets split it up: https://issues.apache.org/jira/browse/PARQUET-310
public class ParquetMetadataConverter {

  private static final TypeDefinedOrder TYPE_DEFINED_ORDER = new TypeDefinedOrder();
  public static final MetadataFilter NO_FILTER = new NoFilter();
  public static final MetadataFilter SKIP_ROW_GROUPS = new SkipMetadataFilter();
  public static final long MAX_STATS_SIZE = 4096; // limit stats to 4k

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataConverter.class);
  private static final LogicalTypeConverterVisitor LOGICAL_TYPE_ANNOTATION_VISITOR = new LogicalTypeConverterVisitor();
  private static final ConvertedTypeConverterVisitor CONVERTED_TYPE_CONVERTER_VISITOR = new ConvertedTypeConverterVisitor();
  private final int statisticsTruncateLength;
  private final boolean useSignedStringMinMax;

  public ParquetMetadataConverter() {
    this(false);
  }

  public ParquetMetadataConverter(int statisticsTruncateLength) {
    this(false, statisticsTruncateLength);
  }

  /**
   * @param conf a configuration
   * @deprecated will be removed in 2.0.0; use {@code ParquetMetadataConverter(ParquetReadOptions)}
   */
  @Deprecated
  public ParquetMetadataConverter(Configuration conf) {
    this(conf.getBoolean("parquet.strings.signed-min-max.enabled", false));
  }

  public ParquetMetadataConverter(ParquetReadOptions options) {
    this(options.useSignedStringMinMax());
  }

  private ParquetMetadataConverter(boolean useSignedStringMinMax) {
    this(useSignedStringMinMax, ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH);
  }

  private ParquetMetadataConverter(boolean useSignedStringMinMax, int statisticsTruncateLength) {
    if (statisticsTruncateLength <= 0) {
      throw new IllegalArgumentException("Truncate length should be greater than 0");
    }
    this.useSignedStringMinMax = useSignedStringMinMax;
    this.statisticsTruncateLength = statisticsTruncateLength;
  }

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

    fileMetaData.setColumn_orders(getColumnOrders(parquetMetadata.getFileMetaData().getSchema()));

    return fileMetaData;
  }

  private List<ColumnOrder> getColumnOrders(MessageType schema) {
    List<ColumnOrder> columnOrders = new ArrayList<>();
    // Currently, only TypeDefinedOrder is supported, so we create a column order for each columns with
    // TypeDefinedOrder even if some types (e.g. INT96) have undefined column orders.
    for (int i = 0, n = schema.getPaths().size(); i < n; ++i) {
      ColumnOrder columnOrder = new ColumnOrder();
      columnOrder.setTYPE_ORDER(TYPE_DEFINED_ORDER);
      columnOrders.add(columnOrder);
    }
    return columnOrders;
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
        if (primitiveType.getLogicalTypeAnnotation() != null) {
          element.setConverted_type(convertToConvertedType(primitiveType.getLogicalTypeAnnotation()));
          element.setLogicalType(convertToLogicalType(primitiveType.getLogicalTypeAnnotation()));
        }
        if (primitiveType.getDecimalMetadata() != null) {
          element.setPrecision(primitiveType.getDecimalMetadata().getPrecision());
          element.setScale(primitiveType.getDecimalMetadata().getScale());
        }
        if (primitiveType.getTypeLength() > 0) {
          element.setType_length(primitiveType.getTypeLength());
        }
        if (primitiveType.getId() != null) {
          element.setField_id(primitiveType.getId().intValue());
        }
        result.add(element);
      }

      @Override
      public void visit(MessageType messageType) {
        SchemaElement element = new SchemaElement(messageType.getName());
        if (messageType.getId() != null) {
          element.setField_id(messageType.getId().intValue());
        }
        visitChildren(result, messageType.asGroupType(), element);
      }

      @Override
      public void visit(GroupType groupType) {
        SchemaElement element = new SchemaElement(groupType.getName());
        element.setRepetition_type(toParquetRepetition(groupType.getRepetition()));
        if (groupType.getLogicalTypeAnnotation() != null) {
          element.setConverted_type(convertToConvertedType(groupType.getLogicalTypeAnnotation()));
          element.setLogicalType(convertToLogicalType(groupType.getLogicalTypeAnnotation()));
        }
        if (groupType.getId() != null) {
          element.setField_id(groupType.getId().intValue());
        }
        visitChildren(result, groupType, element);
      }

      private void visitChildren(final List<SchemaElement> result,
          GroupType groupType, SchemaElement element) {
        element.setNum_children(groupType.getFieldCount());
        result.add(element);
        for (org.apache.parquet.schema.Type field : groupType.getFields()) {
          addToList(result, field);
        }
      }
    });
  }

  LogicalType convertToLogicalType(LogicalTypeAnnotation logicalTypeAnnotation) {
    return logicalTypeAnnotation.accept(LOGICAL_TYPE_ANNOTATION_VISITOR).get();
  }

  ConvertedType convertToConvertedType(LogicalTypeAnnotation logicalTypeAnnotation) {
    return logicalTypeAnnotation.accept(CONVERTED_TYPE_CONVERTER_VISITOR).orElse(null);
  }

  static org.apache.parquet.format.TimeUnit convertUnit(LogicalTypeAnnotation.TimeUnit unit) {
    switch (unit) {
      case MICROS:
        return org.apache.parquet.format.TimeUnit.MICROS(new MicroSeconds());
      case MILLIS:
        return org.apache.parquet.format.TimeUnit.MILLIS(new MilliSeconds());
      case NANOS:
        return TimeUnit.NANOS(new NanoSeconds());
      default:
        throw new RuntimeException("Unknown time unit " + unit);
    }
  }

  private static class ConvertedTypeConverterVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ConvertedType> {
    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
      return of(ConvertedType.UTF8);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
      return of(ConvertedType.MAP);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
      return of(ConvertedType.LIST);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
      return of(ConvertedType.ENUM);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
      return of(ConvertedType.DECIMAL);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
      return of(ConvertedType.DATE);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
      switch (timeLogicalType.getUnit()) {
        case MILLIS:
          return of(ConvertedType.TIME_MILLIS);
        case MICROS:
          return of(ConvertedType.TIME_MICROS);
        case NANOS:
          return empty();
        default:
          throw new RuntimeException("Unknown converted type for " + timeLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
      switch (timestampLogicalType.getUnit()) {
        case MICROS:
          return of(ConvertedType.TIMESTAMP_MICROS);
        case MILLIS:
          return of(ConvertedType.TIMESTAMP_MILLIS);
        case NANOS:
          return empty();
        default:
          throw new RuntimeException("Unknown converted type for " + timestampLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
      boolean signed = intLogicalType.isSigned();
      switch (intLogicalType.getBitWidth()) {
        case 8:
          return of(signed ? ConvertedType.INT_8 : ConvertedType.UINT_8);
        case 16:
          return of(signed ? ConvertedType.INT_16 : ConvertedType.UINT_16);
        case 32:
          return of(signed ? ConvertedType.INT_32 : ConvertedType.UINT_32);
        case 64:
          return of(signed ? ConvertedType.INT_64 : ConvertedType.UINT_64);
        default:
          throw new RuntimeException("Unknown original type " + intLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return of(ConvertedType.JSON);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return of(ConvertedType.BSON);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
      return of(ConvertedType.INTERVAL);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
      return of(ConvertedType.MAP_KEY_VALUE);
    }
  }

  private static class LogicalTypeConverterVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<LogicalType> {
    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
      return of(LogicalType.STRING(new StringType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
      return of(LogicalType.MAP(new MapType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
      return of(LogicalType.LIST(new ListType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
      return of(LogicalType.ENUM(new EnumType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
      return of(LogicalType.DECIMAL(new DecimalType(decimalLogicalType.getScale(), decimalLogicalType.getPrecision())));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
      return of(LogicalType.DATE(new DateType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
      return of(LogicalType.TIME(new TimeType(timeLogicalType.isAdjustedToUTC(), convertUnit(timeLogicalType.getUnit()))));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
      return of(LogicalType.TIMESTAMP(new TimestampType(timestampLogicalType.isAdjustedToUTC(), convertUnit(timestampLogicalType.getUnit()))));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
      return of(LogicalType.INTEGER(new IntType((byte) intLogicalType.getBitWidth(), intLogicalType.isSigned())));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return of(LogicalType.JSON(new JsonType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return of(LogicalType.BSON(new BsonType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
      return of(LogicalType.UNKNOWN(new NullType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
      return of(LogicalType.UNKNOWN(new NullType()));
    }
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
          toFormatCodec(columnMetaData.getCodec()),
          columnMetaData.getValueCount(),
          columnMetaData.getTotalUncompressedSize(),
          columnMetaData.getTotalSize(),
          columnMetaData.getFirstDataPageOffset());
      columnChunk.meta_data.dictionary_page_offset = columnMetaData.getDictionaryPageOffset();
      columnChunk.meta_data.setBloom_filter_offset(columnMetaData.getBloomFilterOffset());
      if (!columnMetaData.getStatistics().isEmpty()) {
        columnChunk.meta_data.setStatistics(toParquetStatistics(columnMetaData.getStatistics(), this.statisticsTruncateLength));
      }
      if (columnMetaData.getEncodingStats() != null) {
        columnChunk.meta_data.setEncoding_stats(convertEncodingStats(columnMetaData.getEncodingStats()));
      }
//      columnChunk.meta_data.index_page_offset = ;
//      columnChunk.meta_data.key_value_metadata = ; // nothing yet

      IndexReference columnIndexRef = columnMetaData.getColumnIndexReference();
      if (columnIndexRef != null) {
        columnChunk.setColumn_index_offset(columnIndexRef.getOffset());
        columnChunk.setColumn_index_length(columnIndexRef.getLength());
      }
      IndexReference offsetIndexRef = columnMetaData.getOffsetIndexReference();
      if (offsetIndexRef != null) {
        columnChunk.setOffset_index_offset(offsetIndexRef.getOffset());
        columnChunk.setOffset_index_length(offsetIndexRef.getLength());
      }

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

  private CompressionCodecName fromFormatCodec(CompressionCodec codec) {
    return CompressionCodecName.valueOf(codec.toString());
  }

  private CompressionCodec toFormatCodec(CompressionCodecName codec) {
    return CompressionCodec.valueOf(codec.toString());
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

  public static Statistics toParquetStatistics(
    org.apache.parquet.column.statistics.Statistics stats) {
    return toParquetStatistics(stats, ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH);
  }

  public static Statistics toParquetStatistics(
      org.apache.parquet.column.statistics.Statistics stats, int truncateLength) {
    Statistics formatStats = new Statistics();
    // Don't write stats larger than the max size rather than truncating. The
    // rationale is that some engines may use the minimum value in the page as
    // the true minimum for aggregations and there is no way to mark that a
    // value has been truncated and is a lower bound and not in the page.
    if (!stats.isEmpty() && withinLimit(stats, truncateLength)) {
      formatStats.setNull_count(stats.getNumNulls());
      if (stats.hasNonNullValue()) {
        byte[] min;
        byte[] max;

        if (stats instanceof BinaryStatistics && truncateLength != Integer.MAX_VALUE) {
          BinaryTruncator truncator = BinaryTruncator.getTruncator(stats.type());
          min = tuncateMin(truncator, truncateLength, stats.getMinBytes());
          max = tuncateMax(truncator, truncateLength, stats.getMaxBytes());
        } else {
          min = stats.getMinBytes();
          max = stats.getMaxBytes();
        }
        // Fill the former min-max statistics only if the comparison logic is
        // signed so the logic of V1 and V2 stats are the same (which is
        // trivially true for equal min-max values)
        if (sortOrder(stats.type()) == SortOrder.SIGNED || Arrays.equals(min, max)) {
          formatStats.setMin(min);
          formatStats.setMax(max);
        }

        if (isMinMaxStatsSupported(stats.type()) || Arrays.equals(min, max)) {
          formatStats.setMin_value(min);
          formatStats.setMax_value(max);
        }
      }
    }
    return formatStats;
  }

  private static boolean withinLimit(org.apache.parquet.column.statistics.Statistics stats, int truncateLength) {
    if (stats.isSmallerThan(MAX_STATS_SIZE)) {
      return true;
    }

    if (!(stats instanceof BinaryStatistics)) {
      return false;
    }

    BinaryStatistics binaryStatistics = (BinaryStatistics) stats;
    return binaryStatistics.isSmallerThanWithTruncation(MAX_STATS_SIZE, truncateLength);
  }

  private static byte[] tuncateMin(BinaryTruncator truncator, int truncateLength, byte[] input) {
    return truncator.truncateMin(Binary.fromConstantByteArray(input), truncateLength).getBytes();
  }

  private static byte[] tuncateMax(BinaryTruncator truncator, int truncateLength, byte[] input) {
    return truncator.truncateMax(Binary.fromConstantByteArray(input), truncateLength).getBytes();
  }

  private static boolean isMinMaxStatsSupported(PrimitiveType type) {
    return type.columnOrder().getColumnOrderName() == ColumnOrderName.TYPE_DEFINED_ORDER;
  }

  /**
   * @param statistics parquet format statistics
   * @param type a primitive type name
   * @return the statistics
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  public static org.apache.parquet.column.statistics.Statistics fromParquetStatistics(Statistics statistics, PrimitiveTypeName type) {
    return fromParquetStatistics(null, statistics, type);
  }

  /**
   * @param createdBy the created-by string from the file
   * @param statistics parquet format statistics
   * @param type a primitive type name
   * @return the statistics
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  public static org.apache.parquet.column.statistics.Statistics fromParquetStatistics
      (String createdBy, Statistics statistics, PrimitiveTypeName type) {
    return fromParquetStatisticsInternal(createdBy, statistics,
        new PrimitiveType(Repetition.OPTIONAL, type, "fake_type"), defaultSortOrder(type));
  }

  // Visible for testing
  static org.apache.parquet.column.statistics.Statistics fromParquetStatisticsInternal
      (String createdBy, Statistics formatStats, PrimitiveType type, SortOrder typeSortOrder) {
    // create stats object based on the column type
    org.apache.parquet.column.statistics.Statistics.Builder statsBuilder =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);

    if (formatStats != null) {
      // Use the new V2 min-max statistics over the former one if it is filled
      if (formatStats.isSetMin_value() && formatStats.isSetMax_value()) {
        byte[] min = formatStats.min_value.array();
        byte[] max = formatStats.max_value.array();
        if (isMinMaxStatsSupported(type) || Arrays.equals(min, max)) {
          statsBuilder.withMin(min);
          statsBuilder.withMax(max);
        }
      } else {
        boolean isSet = formatStats.isSetMax() && formatStats.isSetMin();
        boolean maxEqualsMin = isSet ? Arrays.equals(formatStats.getMin(), formatStats.getMax()) : false;
        boolean sortOrdersMatch = SortOrder.SIGNED == typeSortOrder;
        // NOTE: See docs in CorruptStatistics for explanation of why this check is needed
        // The sort order is checked to avoid returning min/max stats that are not
        // valid with the type's sort order. In previous releases, all stats were
        // aggregated using a signed byte-wise ordering, which isn't valid for all the
        // types (e.g. strings, decimals etc.).
        if (!CorruptStatistics.shouldIgnoreStatistics(createdBy, type.getPrimitiveTypeName()) &&
            (sortOrdersMatch || maxEqualsMin)) {
          if (isSet) {
            statsBuilder.withMin(formatStats.min.array());
            statsBuilder.withMax(formatStats.max.array());
          }
        }
      }

      if (formatStats.isSetNull_count()) {
        statsBuilder.withNumNulls(formatStats.null_count);
      }
    }
    return statsBuilder.build();
  }

  public org.apache.parquet.column.statistics.Statistics fromParquetStatistics(
      String createdBy, Statistics statistics, PrimitiveType type) {
    SortOrder expectedOrder = overrideSortOrderToSigned(type) ?
        SortOrder.SIGNED : sortOrder(type);
    return fromParquetStatisticsInternal(
        createdBy, statistics, type, expectedOrder);
  }

  /**
   * Sort order for page and column statistics. Types are associated with sort
   * orders (e.g., UTF8 columns should use UNSIGNED) and column stats are
   * aggregated using a sort order. As of parquet-format version 2.3.1, the
   * order used to aggregate stats is always SIGNED and is not stored in the
   * Parquet file. These stats are discarded for types that need unsigned.
   *
   * See PARQUET-686.
   */
  enum SortOrder {
    SIGNED,
    UNSIGNED,
    UNKNOWN
  }

  private static final Set<Class> STRING_TYPES = Collections
      .unmodifiableSet(new HashSet<>(Arrays.asList(
        LogicalTypeAnnotation.StringLogicalTypeAnnotation.class,
        LogicalTypeAnnotation.EnumLogicalTypeAnnotation.class,
        LogicalTypeAnnotation.JsonLogicalTypeAnnotation.class
      )));

  /**
   * Returns whether to use signed order min and max with a type. It is safe to
   * use signed min and max when the type is a string type and contains only
   * ASCII characters (where the sign bit was 0). This checks whether the type
   * is a string type and uses {@code useSignedStringMinMax} to determine if
   * only ASCII characters were written.
   *
   * @param type a primitive type with a logical type annotation
   * @return true if signed order min/max can be used with this type
   */
  private boolean overrideSortOrderToSigned(PrimitiveType type) {
    // even if the override is set, only return stats for string-ish types
    // a null type annotation is considered string-ish because some writers
    // failed to use the UTF8 annotation.
    LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
    return useSignedStringMinMax &&
        PrimitiveTypeName.BINARY == type.getPrimitiveTypeName() &&
        (annotation == null || STRING_TYPES.contains(annotation.getClass()));
  }

  /**
   * @param primitive a primitive physical type
   * @return the default sort order used when the logical type is not known
   */
  private static SortOrder defaultSortOrder(PrimitiveTypeName primitive) {
    switch (primitive) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        return SortOrder.SIGNED;
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return SortOrder.UNSIGNED;
    }
    return SortOrder.UNKNOWN;
  }

  /**
   * @param primitive a primitive type with a logical type annotation
   * @return the "correct" sort order of the type that applications assume
   */
  private static SortOrder sortOrder(PrimitiveType primitive) {
    LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
    if (annotation != null) {
      return annotation.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<SortOrder>() {
        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
          return intLogicalType.isSigned() ? of(SortOrder.SIGNED) : of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
          return of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
          return of(SortOrder.SIGNED);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
          return of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
          return of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
          return of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
          return of(SortOrder.UNSIGNED);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
          return of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
          return of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
          return of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
          return of(SortOrder.UNKNOWN);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
          return of(SortOrder.SIGNED);
        }

        @Override
        public Optional<SortOrder> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
          return of(SortOrder.SIGNED);
        }
      }).orElse(defaultSortOrder(primitive.getPrimitiveTypeName()));
    }

    return defaultSortOrder(primitive.getPrimitiveTypeName());
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
  LogicalTypeAnnotation getLogicalTypeAnnotation(ConvertedType type, SchemaElement schemaElement) {
    switch (type) {
      case UTF8:
        return LogicalTypeAnnotation.stringType();
      case MAP:
        return LogicalTypeAnnotation.mapType();
      case MAP_KEY_VALUE:
        return LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance();
      case LIST:
        return LogicalTypeAnnotation.listType();
      case ENUM:
        return LogicalTypeAnnotation.enumType();
      case DECIMAL:
        int scale = (schemaElement == null ? 0 : schemaElement.scale);
        int precision = (schemaElement == null ? 0 : schemaElement.precision);
        return LogicalTypeAnnotation.decimalType(scale, precision);
      case DATE:
        return LogicalTypeAnnotation.dateType();
      case TIME_MILLIS:
        return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIME_MICROS:
        return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case TIMESTAMP_MILLIS:
        return LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIMESTAMP_MICROS:
        return LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case INTERVAL:
        return LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance();
      case INT_8:
        return LogicalTypeAnnotation.intType(8, true);
      case INT_16:
        return LogicalTypeAnnotation.intType(16, true);
      case INT_32:
        return LogicalTypeAnnotation.intType(32, true);
      case INT_64:
        return LogicalTypeAnnotation.intType(64, true);
      case UINT_8:
        return LogicalTypeAnnotation.intType(8, false);
      case UINT_16:
        return LogicalTypeAnnotation.intType(16, false);
      case UINT_32:
        return LogicalTypeAnnotation.intType(32, false);
      case UINT_64:
        return LogicalTypeAnnotation.intType(64, false);
      case JSON:
        return LogicalTypeAnnotation.jsonType();
      case BSON:
        return LogicalTypeAnnotation.bsonType();
      default:
        throw new RuntimeException("Can't convert converted type to logical type, unknown converted type " + type);
    }
  }

  LogicalTypeAnnotation getLogicalTypeAnnotation(LogicalType type) {
    switch (type.getSetField()) {
      case MAP:
        return LogicalTypeAnnotation.mapType();
      case BSON:
        return LogicalTypeAnnotation.bsonType();
      case DATE:
        return LogicalTypeAnnotation.dateType();
      case ENUM:
        return LogicalTypeAnnotation.enumType();
      case JSON:
        return LogicalTypeAnnotation.jsonType();
      case LIST:
        return LogicalTypeAnnotation.listType();
      case TIME:
        TimeType time = type.getTIME();
        return LogicalTypeAnnotation.timeType(time.isAdjustedToUTC, convertTimeUnit(time.unit));
      case STRING:
        return LogicalTypeAnnotation.stringType();
      case DECIMAL:
        DecimalType decimal = type.getDECIMAL();
        return LogicalTypeAnnotation.decimalType(decimal.scale, decimal.precision);
      case INTEGER:
        IntType integer = type.getINTEGER();
        return LogicalTypeAnnotation.intType(integer.bitWidth, integer.isSigned);
      case UNKNOWN:
        return null;
      case TIMESTAMP:
        TimestampType timestamp = type.getTIMESTAMP();
        return LogicalTypeAnnotation.timestampType(timestamp.isAdjustedToUTC, convertTimeUnit(timestamp.unit));
      default:
        throw new RuntimeException("Unknown logical type " + type);
    }
  }

  private LogicalTypeAnnotation.TimeUnit convertTimeUnit(TimeUnit unit) {
    switch (unit.getSetField()) {
      case MICROS:
        return LogicalTypeAnnotation.TimeUnit.MICROS;
      case MILLIS:
        return LogicalTypeAnnotation.TimeUnit.MILLIS;
      case NANOS:
        return LogicalTypeAnnotation.TimeUnit.NANOS;
      default:
        throw new RuntimeException("Unknown time unit " + unit);
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
   * @param startOffset a start offset (inclusive)
   * @param endOffset an end offset (exclusive)
   * @return a range filter from the offsets
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
    return readParquetMetadata(from, NO_FILTER);
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

  public ParquetMetadata readParquetMetadata(final InputStream from, MetadataFilter filter) throws IOException {
    FileMetaData fileMetaData = filter.accept(new MetadataFilterVisitor<FileMetaData, IOException>() {
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
    LOG.debug("{}", fileMetaData);
    ParquetMetadata parquetMetadata = fromParquetMetadata(fileMetaData);
    if (LOG.isDebugEnabled()) LOG.debug(ParquetMetadata.toPrettyJSON(parquetMetadata));
    return parquetMetadata;
  }

  public ParquetMetadata fromParquetMetadata(FileMetaData parquetMetadata) throws IOException {
    MessageType messageType = fromParquetSchema(parquetMetadata.getSchema(), parquetMetadata.getColumn_orders());
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
              messageType.getType(path.toArray()).asPrimitiveType(),
              fromFormatCodec(metaData.codec),
              convertEncodingStats(metaData.getEncoding_stats()),
              fromFormatEncodings(metaData.encodings),
              fromParquetStatistics(
                  parquetMetadata.getCreated_by(),
                  metaData.statistics,
                  messageType.getType(path.toArray()).asPrimitiveType()),
              metaData.data_page_offset,
              metaData.dictionary_page_offset,
              metaData.num_values,
              metaData.total_compressed_size,
              metaData.total_uncompressed_size);
          column.setColumnIndexReference(toColumnIndexReference(columnChunk));
          column.setOffsetIndexReference(toOffsetIndexReference(columnChunk));
          column.setBloomFilterOffset(metaData.bloom_filter_offset);
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

  private static IndexReference toColumnIndexReference(ColumnChunk columnChunk) {
    if (columnChunk.isSetColumn_index_offset() && columnChunk.isSetColumn_index_length()) {
      return new IndexReference(columnChunk.getColumn_index_offset(), columnChunk.getColumn_index_length());
    }
    return null;
  }

  private static IndexReference toOffsetIndexReference(ColumnChunk columnChunk) {
    if (columnChunk.isSetOffset_index_offset() && columnChunk.isSetOffset_index_length()) {
      return new IndexReference(columnChunk.getOffset_index_offset(), columnChunk.getOffset_index_length());
    }
    return null;
  }

  private static ColumnPath getPath(ColumnMetaData metaData) {
    String[] path = metaData.path_in_schema.toArray(new String[0]);
    return ColumnPath.get(path);
  }

  // Visible for testing
  MessageType fromParquetSchema(List<SchemaElement> schema, List<ColumnOrder> columnOrders) {
    Iterator<SchemaElement> iterator = schema.iterator();
    SchemaElement root = iterator.next();
    Types.MessageTypeBuilder builder = Types.buildMessage();
    if (root.isSetField_id()) {
      builder.id(root.field_id);
    }
    buildChildren(builder, iterator, root.getNum_children(), columnOrders, 0);
    return builder.named(root.name);
  }

  private void buildChildren(Types.GroupBuilder builder,
                             Iterator<SchemaElement> schema,
                             int childrenCount,
                             List<ColumnOrder> columnOrders,
                             int columnCount) {
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
        if (columnOrders != null) {
          org.apache.parquet.schema.ColumnOrder columnOrder = fromParquetColumnOrder(columnOrders.get(columnCount));
          // As per parquet format 2.4.0 no UNDEFINED order is supported. So, set undefined column order for the types
          // where ordering is not supported.
          if (columnOrder.getColumnOrderName() == ColumnOrderName.TYPE_DEFINED_ORDER
              && (schemaElement.type == Type.INT96 || schemaElement.converted_type == ConvertedType.INTERVAL)) {
            columnOrder = org.apache.parquet.schema.ColumnOrder.undefined();
          }
          primitiveBuilder.columnOrder(columnOrder);
        }
        childBuilder = primitiveBuilder;

      } else {
        childBuilder = builder.group(fromParquetRepetition(schemaElement.repetition_type));
        buildChildren((Types.GroupBuilder) childBuilder, schema, schemaElement.num_children, columnOrders, columnCount);
      }

      if (schemaElement.isSetLogicalType()) {
        childBuilder.as(getLogicalTypeAnnotation(schemaElement.logicalType));
      }
      if (schemaElement.isSetConverted_type()) {
        OriginalType originalType = getLogicalTypeAnnotation(schemaElement.converted_type, schemaElement).toOriginalType();
        OriginalType newOriginalType = (schemaElement.isSetLogicalType() && getLogicalTypeAnnotation(schemaElement.logicalType) != null) ?
           getLogicalTypeAnnotation(schemaElement.logicalType).toOriginalType() : null;
        if (!originalType.equals(newOriginalType)) {
          if (newOriginalType != null) {
            LOG.warn("Converted type and logical type metadata mismatch (convertedType: {}, logical type: {}). Using value in converted type.",
              schemaElement.converted_type, schemaElement.logicalType);
          }
          childBuilder.as(originalType);
        }
      }
      if (schemaElement.isSetField_id()) {
        childBuilder.id(schemaElement.field_id);
      }

      childBuilder.named(schemaElement.name);
      ++columnCount;
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

  private static org.apache.parquet.schema.ColumnOrder fromParquetColumnOrder(ColumnOrder columnOrder) {
    if (columnOrder.isSetTYPE_ORDER()) {
      return org.apache.parquet.schema.ColumnOrder.typeDefined();
    }
    // The column order is not yet supported by this API
    return org.apache.parquet.schema.ColumnOrder.undefined();
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
                                      rlEncoding,
                                      dlEncoding,
                                      valuesEncoding), to);
  }

  // Statistics are no longer saved in page headers
  @Deprecated
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
        newDataPageHeader(uncompressedSize, compressedSize, valueCount,
            rlEncoding, dlEncoding, valuesEncoding),
        to);
  }

  private PageHeader newDataPageHeader(
    int uncompressedSize, int compressedSize,
    int valueCount,
    org.apache.parquet.column.Encoding rlEncoding,
    org.apache.parquet.column.Encoding dlEncoding,
    org.apache.parquet.column.Encoding valuesEncoding) {
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);
    pageHeader.setData_page_header(new DataPageHeader(
      valueCount,
      getEncoding(valuesEncoding),
      getEncoding(dlEncoding),
      getEncoding(rlEncoding)));
    return pageHeader;
  }

  private PageHeader newDataPageHeader(
      int uncompressedSize, int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      int crc) {
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);
    pageHeader.setCrc(crc);
    pageHeader.setData_page_header(new DataPageHeader(
        valueCount,
        getEncoding(valuesEncoding),
        getEncoding(dlEncoding),
        getEncoding(rlEncoding)));
    return pageHeader;
  }

  // Statistics are no longer saved in page headers
  @Deprecated
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
            dataEncoding,
            rlByteLength, dlByteLength), to);
  }

  public void writeDataPageV1Header(
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
      rlEncoding,
      dlEncoding,
      valuesEncoding), to);
  }

  public void writeDataPageV1Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      int crc,
      OutputStream to) throws IOException {
    writePageHeader(newDataPageHeader(uncompressedSize,
                                      compressedSize,
                                      valueCount,
                                      rlEncoding,
                                      dlEncoding,
                                      valuesEncoding,
                                      crc), to);
  }

  public void writeDataPageV2Header(
      int uncompressedSize, int compressedSize,
      int valueCount, int nullCount, int rowCount,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength, int dlByteLength,
      OutputStream to) throws IOException {
    writePageHeader(
        newDataPageV2Header(
            uncompressedSize, compressedSize,
            valueCount, nullCount, rowCount,
            dataEncoding,
            rlByteLength, dlByteLength), to);
  }

  private PageHeader newDataPageV2Header(
      int uncompressedSize, int compressedSize,
      int valueCount, int nullCount, int rowCount,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength, int dlByteLength) {
    // TODO: pageHeader.crc = ...;
    DataPageHeaderV2 dataPageHeaderV2 = new DataPageHeaderV2(
        valueCount, nullCount, rowCount,
        getEncoding(dataEncoding),
        dlByteLength, rlByteLength);
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

  public void writeDictionaryPageHeader(
      int uncompressedSize, int compressedSize, int valueCount,
      org.apache.parquet.column.Encoding valuesEncoding, int crc, OutputStream to) throws IOException {
    PageHeader pageHeader = new PageHeader(PageType.DICTIONARY_PAGE, uncompressedSize, compressedSize);
    pageHeader.setCrc(crc);
    pageHeader.setDictionary_page_header(new DictionaryPageHeader(valueCount, getEncoding(valuesEncoding)));
    writePageHeader(pageHeader, to);
  }

  private static BoundaryOrder toParquetBoundaryOrder(
      org.apache.parquet.internal.column.columnindex.BoundaryOrder boundaryOrder) {
    switch (boundaryOrder) {
      case ASCENDING:
        return BoundaryOrder.ASCENDING;
      case DESCENDING:
        return BoundaryOrder.DESCENDING;
      case UNORDERED:
        return BoundaryOrder.UNORDERED;
      default:
        throw new IllegalArgumentException("Unsupported boundary order: " + boundaryOrder);
    }
  }

  private static org.apache.parquet.internal.column.columnindex.BoundaryOrder fromParquetBoundaryOrder(
      BoundaryOrder boundaryOrder) {
    switch (boundaryOrder) {
      case ASCENDING:
        return org.apache.parquet.internal.column.columnindex.BoundaryOrder.ASCENDING;
      case DESCENDING:
        return org.apache.parquet.internal.column.columnindex.BoundaryOrder.DESCENDING;
      case UNORDERED:
        return org.apache.parquet.internal.column.columnindex.BoundaryOrder.UNORDERED;
      default:
        throw new IllegalArgumentException("Unsupported boundary order: " + boundaryOrder);
    }
  }

  public static ColumnIndex toParquetColumnIndex(PrimitiveType type,
      org.apache.parquet.internal.column.columnindex.ColumnIndex columnIndex) {
    if (!isMinMaxStatsSupported(type) || columnIndex == null) {
      return null;
    }
    ColumnIndex parquetColumnIndex = new ColumnIndex(
        columnIndex.getNullPages(),
        columnIndex.getMinValues(),
        columnIndex.getMaxValues(),
        toParquetBoundaryOrder(columnIndex.getBoundaryOrder()));
    parquetColumnIndex.setNull_counts(columnIndex.getNullCounts());
    return parquetColumnIndex;
  }

  public static org.apache.parquet.internal.column.columnindex.ColumnIndex fromParquetColumnIndex(PrimitiveType type,
      ColumnIndex parquetColumnIndex) {
    if (!isMinMaxStatsSupported(type)) {
      return null;
    }
    return ColumnIndexBuilder.build(type,
        fromParquetBoundaryOrder(parquetColumnIndex.getBoundary_order()),
        parquetColumnIndex.getNull_pages(),
        parquetColumnIndex.getNull_counts(),
        parquetColumnIndex.getMin_values(),
        parquetColumnIndex.getMax_values());
  }

  public static OffsetIndex toParquetOffsetIndex(org.apache.parquet.internal.column.columnindex.OffsetIndex offsetIndex) {
    List<PageLocation> pageLocations = new ArrayList<>(offsetIndex.getPageCount());
    for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
      pageLocations.add(new PageLocation(
          offsetIndex.getOffset(i),
          offsetIndex.getCompressedPageSize(i),
          offsetIndex.getFirstRowIndex(i)));
    }
    return new OffsetIndex(pageLocations);
  }

  public static org.apache.parquet.internal.column.columnindex.OffsetIndex fromParquetOffsetIndex(
      OffsetIndex parquetOffsetIndex) {
    OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
    for (PageLocation pageLocation : parquetOffsetIndex.getPage_locations()) {
      builder.add(pageLocation.getOffset(), pageLocation.getCompressed_page_size(), pageLocation.getFirst_row_index());
    }
    return builder.build();
  }

  public static BloomFilterHeader toBloomFilterHeader(
    org.apache.parquet.column.values.bloomfilter.BloomFilter bloomFilter) {

    BloomFilterAlgorithm algorithm = null;
    BloomFilterHash hashStrategy = null;
    BloomFilterCompression compression = null;

    if (bloomFilter.getAlgorithm() == BloomFilter.Algorithm.BLOCK) {
      algorithm = BloomFilterAlgorithm.BLOCK(new SplitBlockAlgorithm());
    }

    if (bloomFilter.getHashStrategy() == BloomFilter.HashStrategy.XXH64) {
      hashStrategy = BloomFilterHash.XXHASH(new XxHash());
    }

    if (bloomFilter.getCompression() == BloomFilter.Compression.UNCOMPRESSED) {
      compression = BloomFilterCompression.UNCOMPRESSED(new Uncompressed());
    }

    if (algorithm != null && hashStrategy != null && compression != null) {
      return new BloomFilterHeader(bloomFilter.getBitsetSize(), algorithm, hashStrategy, compression);
    } else {
      throw new IllegalArgumentException(String.format("Failed to build thrift structure for BloomFilterHeader," +
        "algorithm=%s, hash=%s, compression=%s",
        bloomFilter.getAlgorithm(), bloomFilter.getHashStrategy(), bloomFilter.getCompression()));
    }
  }
}
