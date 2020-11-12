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
package org.apache.parquet.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnMasker {
  /**
   *
   * @param reader Reader of source file
   * @param writer Writer of destination file
   * @param meta Metadata of source file
   * @param schema Schema of source file
   * @param paths Column Paths need to be masked
   * @param maskMode Mode to mask
   * @throws IOException
   */
  public void processBlocks(TransParquetFileReader reader, ParquetFileWriter writer, ParquetMetadata meta,
                            MessageType schema, List<String> paths, MaskMode maskMode) throws IOException {
    Set<ColumnPath> nullifyColumns = convertToColumnPaths(paths);
    int blockIndex = 0;
    PageReadStore store = reader.readNextRowGroup();

    while (store != null) {
      writer.startBlock(store.getRowCount());
      List<ColumnChunkMetaData> columnsInOrder = meta.getBlocks().get(blockIndex).getColumns();
      Map<ColumnPath, ColumnDescriptor> descriptorsMap = schema.getColumns().stream().collect(
        Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
      ColumnReadStoreImpl crStore = new ColumnReadStoreImpl(store, new DummyGroupConverter(), schema,
        meta.getFileMetaData().getCreatedBy());

      for (int i = 0; i < columnsInOrder.size(); i += 1) {
        ColumnChunkMetaData chunk = columnsInOrder.get(i);
        ColumnDescriptor descriptor = descriptorsMap.get(chunk.getPath());
        processChunk(descriptor, chunk, crStore, reader, writer, schema, nullifyColumns, maskMode);
      }

      writer.endBlock();
      store = reader.readNextRowGroup();
      blockIndex++;
    }
  }

  private void processChunk(ColumnDescriptor descriptor, ColumnChunkMetaData chunk, ColumnReadStoreImpl crStore,
                            TransParquetFileReader reader, ParquetFileWriter writer, MessageType schema,
                            Set<ColumnPath> paths, MaskMode maskMode) throws IOException {
    reader.setStreamPosition(chunk.getStartingPos());

    if (paths.contains(chunk.getPath())) {
      if (maskMode.equals(MaskMode.NULLIFY)) {
        Type.Repetition repetition = descriptor.getPrimitiveType().getRepetition();
        if (repetition.equals(Type.Repetition.REQUIRED)) {
          throw new IOException("Required column [" + descriptor.getPrimitiveType().getName() + "] cannot be nullified");
        }
        nullifyColumn(descriptor, chunk, crStore, writer, schema);
      } else {
        throw new UnsupportedOperationException("Only nullify is supported for now");
      }
    } else {
      BloomFilter bloomFilter = reader.readBloomFilter(chunk);
      ColumnIndex columnIndex = reader.readColumnIndex(chunk);
      OffsetIndex offsetIndex = reader.readOffsetIndex(chunk);
      writer.appendColumnChunk(descriptor, reader.getStream(), chunk, bloomFilter, columnIndex, offsetIndex);
    }
  }

  private void nullifyColumn(ColumnDescriptor descriptor, ColumnChunkMetaData chunk, ColumnReadStoreImpl crStore,
                             ParquetFileWriter writer, MessageType schema) throws IOException {
    long totalChunkValues = chunk.getValueCount();
    int dMax = descriptor.getMaxDefinitionLevel();
    ColumnReader cReader = crStore.getColumnReader(descriptor);

    WriterVersion writerVersion = chunk.getEncodingStats().usesV2Pages() ? WriterVersion.PARQUET_2_0 : WriterVersion.PARQUET_1_0;
    ParquetProperties props = ParquetProperties.builder()
      .withWriterVersion(writerVersion)
      .build();
    CodecFactory codecFactory = new CodecFactory(new Configuration(), props.getPageSizeThreshold());
    CodecFactory.BytesCompressor compressor =	codecFactory.getCompressor(chunk.getCodec());
    
    // Create new schema that only has the current column
    MessageType newSchema = newSchema(schema, descriptor);
    ColumnChunkPageWriteStore cPageStore = new ColumnChunkPageWriteStore(compressor, newSchema, props.getAllocator(), props.getColumnIndexTruncateLength());
    ColumnWriteStore cStore = props.newColumnWriteStore(newSchema, cPageStore);
    ColumnWriter cWriter = cStore.getColumnWriter(descriptor);

    for (int i = 0; i < totalChunkValues; i++) {
      int rlvl = cReader.getCurrentRepetitionLevel();
      int dlvl = cReader.getCurrentDefinitionLevel();
      if (dlvl == dMax) {
        // since we checked ether optional or repeated, dlvl should be > 0
        if (dlvl == 0) {
          throw new IOException("definition level is detected to be 0 for column " + chunk.getPath().toDotString() + " to be nullified");
        }
        // we just write one null for the whole list at the top level, instead of nullify the elements in the list one by one
        if (rlvl == 0) {
          cWriter.writeNull(rlvl, dlvl - 1);
        }
      } else {
        cWriter.writeNull(rlvl, dlvl);
      }
      cStore.endRecord();
    }

    cStore.flush();
    cPageStore.flushToFileWriter(writer);

    cStore.close();
    cWriter.close();
  }
  
  private MessageType newSchema(MessageType schema, ColumnDescriptor descriptor) {
    String[] path = descriptor.getPath();
    Type type = schema.getType(path);
    if (path.length == 1) {
      return new MessageType(schema.getName(), type);
    }

    for (Type field : schema.getFields()) {
      if (!field.isPrimitive()) {
        Type newType = extractField(field.asGroupType(), type);
        if (newType != null) {
          return new MessageType(schema.getName(), newType);
        }
      }
    }

    // We should never hit this because 'type' is returned by schema.getType().
    throw new RuntimeException("No field is found");
  }

  private Type extractField(GroupType candidate, Type targetField) {
    if (targetField.equals(candidate)) {
      return targetField;
    }

    // In case 'type' is a descendants of candidate
    for (Type field : candidate.asGroupType().getFields()) {
      if (field.isPrimitive()) {
        if (field.equals(targetField)) {
          return new GroupType(candidate.getRepetition(), candidate.getName(), targetField);
        }
      } else {
        Type tempField = extractField(field.asGroupType(), targetField);
        if (tempField != null) {
          return tempField;
        }
      }
    }

    return null;
  }

  public static Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
  }

  public enum MaskMode {
    NULLIFY("nullify"),
    HASH("hash"),
    REDACT("redact");

    private String mode;

    MaskMode(String text) {
      this.mode = text;
    }

    public String getMode() {
      return this.mode;
    }

    public static MaskMode fromString(String mode) {
      for (MaskMode b : MaskMode.values()) {
        if (b.mode.equalsIgnoreCase(mode)) {
          return b;
        }
      }
      return null;
    }
  }

  private static final class DummyGroupConverter extends GroupConverter {
    @Override public void start() {}
    @Override public void end() {}
    @Override public Converter getConverter(int fieldIndex) { return new DummyConverter(); }
  }

  private static final class DummyConverter extends PrimitiveConverter {
    @Override public GroupConverter asGroupConverter() { return new DummyGroupConverter(); }
  }
}
