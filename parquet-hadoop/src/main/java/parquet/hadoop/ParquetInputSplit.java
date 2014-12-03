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
package parquet.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import parquet.bytes.BytesUtils;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 * An input split for the Parquet format
 * It contains the information to read one block of the file.
 *
 * This class is private to the ParquetInputFormat.
 * Backward compatibility is not maintained.
 *
 * @author Julien Le Dem
 */
@Private
public class ParquetInputSplit extends FileSplit implements Writable {


  private long end;
  private long[] rowGroupOffsets;

  /**
   * Writables must have a parameterless constructor
   */
  public ParquetInputSplit() {
        super(null, 0, 0, new String[0]);
  }

  /**
   * For compatibility only
   * use {@link ParquetInputSplit#ParquetInputSplit(Path, long, long, long, String[], long[])}
   * @param path
   * @param start
   * @param length
   * @param hosts
   * @param blocks
   * @param requestedSchema
   * @param fileSchema
   * @param extraMetadata
   * @param readSupportMetadata
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
    this(path, start, length, end(blocks, requestedSchema), hosts, offsets(blocks));
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
   * @param file the path of the file for that split
   * @param start the start offset in the file
   * @param end the end offset in the file
   * @param length the actual size in bytes that we expect to read
   * @param hosts the hosts with the replicas of this data
   * @param rowGroupOffsets the offsets of the rowgroups selected if loaded on the client
   */
  public ParquetInputSplit(
      Path file, long start, long end, long length, String[] hosts,
      long[] rowGroupOffsets) {
    super(file, start, length, hosts);
    this.end = end;
    this.rowGroupOffsets = rowGroupOffsets;
  }

  /**
   * @return the end offset of that split
   */
  public long getEnd() {
    return end;
  }

  /**
   * @return the offsets of the row group selected if this has been determined on the client side
   */
  public long[] getRowGroupOffsets() {
    return rowGroupOffsets;
  }

  @Override
  public String toString() {
    String hosts;
    try{
       hosts = Arrays.toString(getLocations());
    } catch (Exception e) {
      // IOException/InterruptedException could be thrown
      hosts = "(" + e + ")";
    }

    return this.getClass().getSimpleName() + "{" +
           "part: " + getPath()
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
