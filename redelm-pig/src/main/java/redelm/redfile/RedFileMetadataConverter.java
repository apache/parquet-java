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
package redelm.redfile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redelm.hadoop.metadata.BlockMetaData;
import redelm.hadoop.metadata.ColumnChunkMetaData;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.hadoop.metadata.RedelmMetaData;
import redelm.parser.MessageTypeParser;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType.Primitive;
import redfile.ColumnChunk;
import redfile.CompressionCodec;
import redfile.Encoding;
import redfile.FileMetaData;
import redfile.KeyValue;
import redfile.PageHeader;
import redfile.RowGroup;
import redfile.Type;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

public class RedFileMetadataConverter {

  public FileMetaData toRedFileMetadata(int currentVersion, RedelmMetaData redelmMetadata) {
    FileMetaData fileMetaData = new FileMetaData();
    fileMetaData.schema = redelmMetadata.getFileMetaData().getSchema();
    fileMetaData.version = currentVersion;
    List<BlockMetaData> blocks = redelmMetadata.getBlocks();
    int numRows = 0;
    for (BlockMetaData block : blocks) {
      numRows += block.getRowCount();
      addRowGroup(redelmMetadata, fileMetaData, block);
    }
    fileMetaData.num_rows = numRows;

    Set<Entry<String, String>> keyValues = redelmMetadata.getKeyValueMetaData().entrySet();
    for (Entry<String, String> keyValue : keyValues) {
      addKeyValue(fileMetaData, keyValue.getKey(), keyValue.getValue());
    }

    return fileMetaData;
  }

  private void addRowGroup(RedelmMetaData redelmMetadata, FileMetaData fileMetaData, BlockMetaData block) {
    RowGroup rowGroup = new RowGroup();
    //rowGroup.total_byte_size = ;
    List<ColumnChunkMetaData> columns = block.getColumns();
    for (ColumnChunkMetaData columnMetaData : columns) {
      ColumnChunk columnChunk = new ColumnChunk();
      columnChunk.file_path = null; // same file
      columnChunk.file_offset = columnMetaData.getFirstDataPage(); // TODO ?
      columnChunk.meta_data = new redfile.ColumnMetaData(
          getType(columnMetaData.getType()),
          Arrays.asList(Encoding.PLAIN), // TODO: deal with encodings
          Arrays.asList(columnMetaData.getPath()),
          columnMetaData.getCodec().getRedFileCompressionCodec(),
          columnMetaData.getValueCount(),
          columnMetaData.getTotalUncompressedSize(),
          columnMetaData.getTotalSize(),
          columnMetaData.getFirstDataPage()
          );
//      columnChunk.meta_data.index_page_offset = ;
//      columnChunk.meta_data.key_value_metadata = ; // nothing yet

      rowGroup.addToColumns(columnChunk);
    }
    fileMetaData.addToRow_groups(rowGroup);
  }

  private CompressionCodec getCodec(String codecClassName) {
    if (codecClassName.equals("org.apache.hadoop.io.compress.GzipCodec")) {
      return CompressionCodec.GZIP;
    } else if (codecClassName.equals("com.hadoop.compression.lzo.LzopCodec")) {
      return CompressionCodec.LZO;
    } else if (codecClassName.equals("org.apache.hadoop.io.compress.SnappyCodec")) {
      return CompressionCodec.SNAPPY;
    } else if (codecClassName.equals("")) {
      return CompressionCodec.UNCOMPRESSED;
    } else {
      throw new RuntimeException("Unknown Codec "+ codecClassName);
    }
  }

  private Type getType(Primitive type) {
    switch (type) {
      case STRING:
        return Type.BYTE_ARRAY;
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
      default:
        throw new RuntimeException("Unknown type " + type);
    }
  }

  private void addKeyValue(FileMetaData fileMetaData, String key, String value) {
    KeyValue keyValue = new KeyValue(key);
    keyValue.value = value;
    fileMetaData.addToKey_value_metadata(keyValue);
  }

  public RedelmMetaData fromRedFileMetadata(FileMetaData redFileMetadata) throws IOException {
//    List<MetaDataBlock> result = new ArrayList<MetaDataBlock>();
    String schema = redFileMetadata.getSchema();
    MessageType messageType = MessageTypeParser.parseMessageType(schema);
    redelm.hadoop.metadata.FileMetaData fileMetadata = new redelm.hadoop.metadata.FileMetaData(schema);
    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
    List<RowGroup> row_groups = redFileMetadata.getRow_groups();
    for (RowGroup rowGroup : row_groups) {
      BlockMetaData blockMetaData = new BlockMetaData();
      blockMetaData.setRowCount(rowGroup.getNum_rows());
      blockMetaData.setTotalByteSize(rowGroup.getTotal_byte_size());
      List<ColumnChunk> columns = rowGroup.getColumns();
      for (ColumnChunk columnChunk : columns) {
        redfile.ColumnMetaData metaData = columnChunk.meta_data;
        String[] path = metaData.path_in_schema.toArray(new String[metaData.path_in_schema.size()]);
        ColumnChunkMetaData column = new ColumnChunkMetaData(path, messageType.getType(path).asPrimitiveType().getPrimitive(), CompressionCodecName.fromRedFile(metaData.codec));
        column.setFirstDataPage(metaData.data_page_offset);
        column.setValueCount(metaData.num_values);
        column.setTotalUncompressedSize(metaData.total_uncompressed_size);
        column.setTotalSize(metaData.total_compressed_size);
        // TODO
        // encodings
        // index_page_offset
        // key_value_metadata
        blockMetaData.addColumn(column);
      }
      blocks.add(blockMetaData);
    }
    Map<String, String> keyValueMetaData = new HashMap<String, String>();
    List<KeyValue> key_value_metadata = redFileMetadata.getKey_value_metadata();
    if (key_value_metadata != null) {
      for (KeyValue keyValue : key_value_metadata) {
        keyValueMetaData.put(keyValue.key, keyValue.value);
      }
    }
    return new RedelmMetaData(fileMetadata, blocks, keyValueMetaData);
  }

  public void writePageHeader(PageHeader pageHeader, OutputStream to) throws IOException {
    write(pageHeader, to);
  }

  public PageHeader readPageHeader(InputStream from) throws IOException {
    return read(from, new PageHeader());
  }

  public void writeFileMetaData(redfile.FileMetaData fileMetadata, OutputStream to) throws IOException {
    write(fileMetadata, to);
  }

  public redfile.FileMetaData readFileMetaData(InputStream from) throws IOException {
    return read(from, new redfile.FileMetaData());
  }

  private TCompactProtocol protocol(OutputStream to) {
    return new TCompactProtocol(new TIOStreamTransport(to));
  }

  private TCompactProtocol protocol(InputStream from) {
    return new TCompactProtocol(new TIOStreamTransport(from));
  }

  private <T extends TBase<?,?>> T read(InputStream from, T tbase)
      throws IOException {
    try {
      tbase.read(protocol(from));
      return tbase;
    } catch (TException e) {
      throw new IOException("can not read " + tbase.getClass() + ": " + e.getMessage(), e);
    }
  }

  private void write(TBase<?, ?> tbase, OutputStream to)
      throws IOException {
    try {
      tbase.write(protocol(to));
    } catch (TException e) {
      throw new IOException("can not write " + tbase, e);
    }
  }

}
