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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * An input split for the Parquet format
 * It contains the information to read one block of the file.
 * <p>
 * This class is private to the ParquetInputFormat.
 * Backward compatibility is not maintained.
 *
 * @deprecated will be removed in 2.0.0. use org.apache.hadoop.mapred.FileSplit instead.
 */
@Private
@Deprecated
public class ParquetInputSplit extends FileSplit implements Writable {

  private long end;
  private long[] rowGroupOffsets;
  private volatile ParquetMetadata footer;

  /**
   * Writables must have a parameterless constructor
   */
  public ParquetInputSplit() {
    super(null, 0, 0, new String[0]);
  }

  /**
   * For compatibility only
   * use {@link ParquetInputSplit#ParquetInputSplit(Path, long, long, long, String[], long[])}
   *
   * @param path                a Path
   * @param start               split start location
   * @param length              split length
   * @param hosts               locality information for this split
   * @param blocks              Parquet blocks in this split
   * @param requestedSchema     the requested schema
   * @param fileSchema          the file schema
   * @param extraMetadata       string map of file metadata
   * @param readSupportMetadata string map of metadata from read support
   */
  @Deprecated
  public ParquetInputSplit(
      Path path,
      long start,
      long length,
      String[] hosts,
      List<BlockMetaData> blocks,
      String requestedSchema,
      String fileSchema,
      Map<String, String> extraMetadata,
      Map<String, String> readSupportMetadata) {
    this(path, start, end(blocks, requestedSchema), length, hosts, offsets(blocks));
  }

  private static long end(List<BlockMetaData> blocks, String requestedSchema) {
    MessageType requested = MessageTypeParser.parseMessageType(requestedSchema);
    long length = 0;

    for (BlockMetaData block : blocks) {
      List<ColumnChunkMetaData> columns = block.getColumns();
      for (ColumnChunkMetaData column : columns) {
        if (requested.containsPath(column.getPath().toArray())) {
          length += column.getTotalSize();
        }
      }
    }
    return length;
  }

  private static long[] offsets(List<BlockMetaData> blocks) {
    long[] offsets = new long[blocks.size()];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = blocks.get(i).getStartingPos();
    }
    return offsets;
  }

  /**
   * @return the block meta data
   * @deprecated the file footer is no longer read before creating input splits
   */
  @Deprecated
  public List<BlockMetaData> getBlocks() {
    throw new UnsupportedOperationException("Splits no longer have row group metadata, see PARQUET-234");
  }

  /**
   * Builds a {@code ParquetInputSplit} from a mapreduce {@link FileSplit}.
   *
   * @param split a mapreduce FileSplit
   * @return a ParquetInputSplit
   * @throws IOException if there is an error while creating the Parquet split
   */
  static ParquetInputSplit from(FileSplit split) throws IOException {
    return new ParquetInputSplit(
        split.getPath(),
        split.getStart(),
        split.getStart() + split.getLength(),
        split.getLength(),
        split.getLocations(),
        null);
  }

  /**
   * Builds a {@code ParquetInputSplit} from a mapred
   * {@link org.apache.hadoop.mapred.FileSplit}.
   *
   * @param split a mapred FileSplit
   * @return a ParquetInputSplit
   * @throws IOException if there is an error while creating the Parquet split
   */
  static ParquetInputSplit from(org.apache.hadoop.mapred.FileSplit split) throws IOException {
    return new ParquetInputSplit(
        split.getPath(),
        split.getStart(),
        split.getStart() + split.getLength(),
        split.getLength(),
        split.getLocations(),
        null);
  }

  /**
   * @param file            the path of the file for that split
   * @param start           the start offset in the file
   * @param end             the end offset in the file
   * @param length          the actual size in bytes that we expect to read
   * @param hosts           the hosts with the replicas of this data
   * @param rowGroupOffsets the offsets of the rowgroups selected if loaded on the client
   */
  public ParquetInputSplit(Path file, long start, long end, long length, String[] hosts, long[] rowGroupOffsets) {
    super(file, start, length, hosts);
    this.end = end;
    this.rowGroupOffsets = rowGroupOffsets;
  }

  /**
   * @return the requested schema
   * @deprecated the file footer is no longer read before creating input splits
   */
  @Deprecated
  String getRequestedSchema() {
    throw new UnsupportedOperationException("Splits no longer have the requested schema, see PARQUET-234");
  }

  /**
   * @return the file schema
   * @deprecated the file footer is no longer read before creating input splits
   */
  @Deprecated
  public String getFileSchema() {
    throw new UnsupportedOperationException("Splits no longer have the file schema, see PARQUET-234");
  }

  /**
   * @return the end offset of that split
   */
  public long getEnd() {
    return end;
  }

  /**
   * @return app specific metadata from the file
   * @deprecated will be removed in 2.0.0. the file footer is no longer read before creating input splits
   */
  @Deprecated
  public Map<String, String> getExtraMetadata() {
    throw new UnsupportedOperationException("Splits no longer have file metadata, see PARQUET-234");
  }

  /**
   * @return app specific metadata provided by the read support in the init phase
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  Map<String, String> getReadSupportMetadata() {
    throw new UnsupportedOperationException("Splits no longer have read-support metadata, see PARQUET-234");
  }

  /**
   * @return the offsets of the row group selected if this has been determined on the client side
   * @deprecated will be removed in 2.0.0.
   */
  public long[] getRowGroupOffsets() {
    return rowGroupOffsets;
  }

  public ParquetMetadata getFooter() {
    return footer;
  }

  public void setFooter(ParquetMetadata footer) {
    this.footer = footer;
  }

  @Override
  public String toString() {
    String hosts;
    try {
      hosts = Arrays.toString(getLocations());
    } catch (Exception e) {
      // IOException/InterruptedException could be thrown
      hosts = "(" + e + ")";
    }

    return this.getClass().getSimpleName() + "{" + "part: "
        + getPath()
        + " start: " + getStart()
        + " end: " + getEnd()
        + " length: " + getLength()
        + " hosts: " + hosts
        + (rowGroupOffsets == null ? "" : (" row groups: " + Arrays.toString(rowGroupOffsets)))
        + "}";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput hin) throws IOException {
    byte[] bytes = readArray(hin);
    DataInputStream in = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)));
    super.readFields(in);
    this.end = in.readLong();
    if (in.readBoolean()) {
      this.rowGroupOffsets = new long[in.readInt()];
      for (int i = 0; i < rowGroupOffsets.length; i++) {
        rowGroupOffsets[i] = in.readLong();
      }
    }
    in.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput hout) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(new GZIPOutputStream(baos));
    super.write(out);
    out.writeLong(end);
    out.writeBoolean(rowGroupOffsets != null);
    if (rowGroupOffsets != null) {
      out.writeInt(rowGroupOffsets.length);
      for (long o : rowGroupOffsets) {
        out.writeLong(o);
      }
    }
    out.close();
    writeArray(hout, baos.toByteArray());
  }

  private static void writeArray(DataOutput out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  private static byte[] readArray(DataInput in) throws IOException {
    int len = in.readInt();
    byte[] bytes = new byte[len];
    in.readFully(bytes);
    return bytes;
  }
}
