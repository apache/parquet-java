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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import redelm.hadoop.RedelmMetaData.FileMetaData;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * An input split for the RedElm format
 * It contains the information to read one block of the file.
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized tuples
 */
public class RedelmInputSplit<T> extends InputSplit implements Serializable, Writable {
  private static final long serialVersionUID = 1L;

  private String path;
  private long start;
  private long length;
  private String[] hosts;
  private BlockMetaData block;
  private FileMetaData fileMetaData;
  private ReadSupport<T> readSupport;

  /**
   * Writables must have a parameterless constructor
   */
  public RedelmInputSplit() {
  }

  /**
   * Used by {@link RedelmInputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)}
   * @param path the path to the file
   * @param start the offset of the block in the file
   * @param length the size of the block in the file
   * @param hosts the hosts where this block can be found
   * @param block the block meta data (Columns locations)
   * @param fileMetaData the file level metadata (Codec, Schema, ...)
   * @param readSupport the object used to materialize records (must be serializable)
   */
  public RedelmInputSplit(Path path, long start, long length, String[] hosts, BlockMetaData block, FileMetaData fileMetaData, ReadSupport<T> readSupport) {
    this.path = path.toUri().toString();
    this.start = start;
    this.length = length;
    this.hosts = hosts;
    this.block = block;
    this.fileMetaData = fileMetaData;
    this.readSupport = readSupport;
  }

  /**
   *
   * @return the block meta data
   */
  public BlockMetaData getBlock() {
    return block;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLength() throws IOException, InterruptedException {
    return length;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return hosts;
  }

  /**
   *
   * @return the offset of the block in the file
   */
  public long getStart() {
    return start;
  }

  /**
   *
   * @return the path of the file containing the block
   */
  public Path getPath() {
    try {
      return new Path(new URI(path));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int l = in.readInt();
    byte[] b = new byte[l];
    in.readFully(b);
    try {
      @SuppressWarnings("unchecked") // I know
      RedelmInputSplit<T> other = (RedelmInputSplit<T>)
          new ObjectInputStream(new ByteArrayInputStream(b))
      .readObject();
      this.path = other.path;
      this.start = other.start;
      this.length = other.length;
      this.hosts = other.hosts;
      this.block = other.block;
      this.fileMetaData = other.fileMetaData;
      this.readSupport = other.readSupport;
    } catch (ClassNotFoundException e) {
      throw new IOException("wrong class serialized", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new ObjectOutputStream(baos).writeObject(this);
    byte[] b = baos.toByteArray();
    out.writeInt(b.length);
    out.write(b);
  }

  /**
   *
   * @return the file level meta data
   */
  public FileMetaData getFileMetaData() {
    return fileMetaData;
  }

  /**
   *
   * @return the object used to materialize records
   */
  public ReadSupport<T> getReadSupport() {
    return readSupport;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" +
           "part: " + path
        + " start: " + start
        + " length: " + length
        + " hosts: " + Arrays.toString(hosts)
        + " block: " + block
        + " fileMetaData: " + fileMetaData
        + "}";
  }
}
