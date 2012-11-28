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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import redelm.Log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Reads a RedElm file
 * @author Julien Le Dem
 *
 */
public class RedelmFileReader {
  private static final Log LOG = Log.getLog(RedelmFileReader.class);

  private static List<MetaDataBlock> readMetaDataBlocks(FSDataInputStream f) throws IOException {
    List<MetaDataBlock> blocks = new ArrayList<MetaDataBlock>();
    int blockCount =  f.read();
    for (int i = 0; i < blockCount; i++) {
      String name = f.readUTF();
      byte[] data = new byte[f.readInt()];
      f.readFully(data);
      blocks.add(new MetaDataBlock(name, data));
    }
    return blocks;
  }

  /**
   * Reads the meta data block in the footer of the file
   * @see RedelmMetaData#fromMetaDataBlocks(List)
   * @param f the RedElm File
   * @param l the length of the file
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   */
  public static final List<MetaDataBlock> readFooter(FSDataInputStream f, long l) throws IOException {
    if (l <= 3 * 8) { // MAGIC (8) + data + footer + footerIndex (8) + MAGIC (8)
      throw new RuntimeException("Not a Red Elm file");
    }
    long footerIndexIndex = l - 8 - 8;
    LOG.debug("reading footer index at " + footerIndexIndex);
    f.seek(footerIndexIndex);
    long footerIndex = f.readLong();
    byte[] magic = new byte[8];
    f.readFully(magic);
    if (!Arrays.equals(RedelmFileWriter.MAGIC, magic)) {
      throw new RuntimeException("Not a Red Elm file");
    }
    LOG.debug("read footer index: " + footerIndex + ", footer size: " + (footerIndexIndex - footerIndex));
    f.seek(footerIndex);
    int version = f.readInt();
    if (version != RedelmFileWriter.CURRENT_VERSION) {
      throw new RuntimeException(
          "unsupported version: " + version + ". " +
          "supporting up to " + RedelmFileWriter.CURRENT_VERSION);
    }

    return readMetaDataBlocks(f);

  }

  private final List<BlockMetaData> blocks;
  private final FSDataInputStream f;
  private int currentBlock = 0;
  private Set<String> paths = new HashSet<String>();
  private long previousReadIndex = 0;
  private final CompressionCodec codec;

  /**
   *
   * @param f the redelm file
   * @param blocks the blocks to read
   * @param colums the columns to read (their path)
   * @param codecClassName the codec used to compress the blocks
   */
  public RedelmFileReader(FSDataInputStream f, List<BlockMetaData> blocks, List<String[]> colums, String codecClassName) {
    this.f = f;
    this.blocks = blocks;
    for (String[] path : colums) {
      paths.add(Arrays.toString(path));
    }
    if (codecClassName == null) {
      this.codec = null;
    } else {
      try {
        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        this.codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Should not happen", e);
      }
    }
  }

  /**
   * reads all the columns requested in the next block
   * @return the block data for the next block
   * @throws IOException if an error occurs while reading
   */
  public BlockData readColumns() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    List<ColumnData> result = new ArrayList<ColumnData>();
    BlockMetaData block = blocks.get(currentBlock);
    LOG.info("starting reading block " + currentBlock);
    long t0 = System.currentTimeMillis();
    for (ColumnMetaData mc : block.getColumns()) {
      String pathKey = Arrays.toString(mc.getPath());
      if (paths.contains(pathKey)) {
        byte[] repetitionLevels = read(pathKey + ".r", mc.getRepetitionStart(), mc.getRepetitionLength());
        byte[] definitionLevels = read(pathKey + ".d", mc.getDefinitionStart(), mc.getDefinitionLength());
        byte[] data = read(pathKey + ".data", mc.getDataStart(), mc.getDataLength());
        result.add(new ColumnData(mc.getPath(), repetitionLevels, definitionLevels, data));
      }
    }
    long t1 = System.currentTimeMillis();
    LOG.info("block data read in " + (t1 - t0) + " ms");

    ++currentBlock;
    return new BlockData(block.getRecordCount(), result);
  }

  private byte[] read(String name, long start, long length) throws IOException {
    byte[] data = new byte[(int)length];
    if (start != previousReadIndex) {
      LOG.info("seeking to next column " + (start - previousReadIndex) + " bytes");
    }
    long t0 = System.currentTimeMillis();
    f.readFully(start, data);
    long t1 = System.currentTimeMillis();
    long l = t1 - t0;
    LOG.info("Read " + length + " bytes for column " + name + " in " + l + " ms" +  (l == 0 ? "" : " : " + (float)data.length*1000/l + " bytes/s"));
    byte[] result;
    if (codec == null) {
      result = data;
    } else {
      Decompressor decompressor = CodecPool.getDecompressor(codec);
      try {
        // TODO make this streaming instead of reading into an array to decompress
        CompressionInputStream cis = codec.createInputStream(new ByteArrayInputStream(data), decompressor);
        ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
        IOUtils.copyBytes(cis, decompressed, 4096, false);
        result = decompressed.toByteArray();
      } finally {
        CodecPool.returnDecompressor(decompressor);
      }
    }
    previousReadIndex = start + length;
    return result;
  }

}
