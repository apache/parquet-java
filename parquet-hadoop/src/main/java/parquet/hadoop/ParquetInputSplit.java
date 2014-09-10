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
  private String requestedSchema;
  private Map<String, String> readSupportMetadata;

  /**
   * Writables must have a parameterless constructor
   */
  public ParquetInputSplit() {
        super(null, 0, 0, new String[0]);
  }

  /**
   * For compatibility only
   * use {@link ParquetInputSplit#ParquetInputSplit(Path, long, long, long, String[], long[], String, Map)}
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
    this(
        path, start, length, end(blocks), hosts,
        offsets(blocks),
        requestedSchema, readSupportMetadata
        );
  }

  private static long end(List<BlockMetaData> blocks) {
    BlockMetaData last = blocks.get(blocks.size() - 1);
    return last.getStartingPos() + last.getCompressedSize();
  }

  private static long[] offsets(List<BlockMetaData> blocks) {
    long[] offsets = new long[blocks.size()];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = blocks.get(0).getStartingPos();
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
   * @param requestedSchema the user requested schema
   * @param readSupportMetadata metadata from the read support
   */
  public ParquetInputSplit(
      Path file, long start, long end, long length, String[] hosts,
      long[] rowGroupOffsets,
      String requestedSchema,
      Map<String, String> readSupportMetadata) {
    super(file, start, length, hosts);
    this.end = end;
    this.rowGroupOffsets = rowGroupOffsets;
    this.requestedSchema = requestedSchema;
    this.readSupportMetadata = readSupportMetadata;
  }

  /**
   * @return the requested schema
   */
  String getRequestedSchema() {
    return requestedSchema;
  }

  /**
   * @return the end offset of that split
   */
  long getEnd() {
    return end;
  }

  /**
   * @return app specific metadata provided by the read support in the init phase
   */
  Map<String, String> getReadSupportMetadata() {
    return readSupportMetadata;
  }

  /**
   * @return the offsets of the row group selected if this has been determined on the client side
   */
  long[] getRowGroupOffsets() {
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
        + " requestedSchema: " +  requestedSchema
        + " readSupportMetadata: " + readSupportMetadata
        + "}";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  final public void readFields(DataInput hin) throws IOException {
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
    this.requestedSchema = readUTF8(in);
    this.readSupportMetadata = readKeyValues(in);
    in.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  final public void write(DataOutput hout) throws IOException {
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
    writeUTF8(out, requestedSchema);
    writeKeyValues(out, readSupportMetadata);
    out.close();
    writeArray(hout, baos.toByteArray());
  }

  private static void writeUTF8(DataOutput out, String string) throws IOException {
    byte[] bytes = string.getBytes(BytesUtils.UTF8);
    writeArray(out, bytes);
  }

  private static String readUTF8(DataInput in) throws IOException {
    byte[] bytes = readArray(in);
    return new String(bytes, BytesUtils.UTF8).intern();
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

  private Map<String, String> readKeyValues(DataInput in) throws IOException {
    int size = in.readInt();
    Map<String, String> map = new HashMap<String, String>(size);
    for (int i = 0; i < size; i++) {
      String key = readUTF8(in).intern();
      String value = readUTF8(in).intern();
      map.put(key, value);
    }
    return map;
  }

  private void writeKeyValues(DataOutput out, Map<String, String> map) throws IOException {
    if (map == null) {
      out.writeInt(0);
    } else {
      out.writeInt(map.size());
      for (Entry<String, String> entry : map.entrySet()) {
        writeUTF8(out, entry.getKey());
        writeUTF8(out, entry.getValue());
      }
    }
  }

}
