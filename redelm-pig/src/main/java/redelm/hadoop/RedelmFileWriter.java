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

import static redelm.Log.INFO;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import redelm.Log;
import redelm.column.BytesOutput;
import redelm.column.ColumnDescriptor;
import redelm.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Writes a RedElm file
 * @author Julien Le Dem
 *
 */
public class RedelmFileWriter extends BytesOutput {
  public static final String RED_ELM_SUMMARY = "_RedElmSummary";
  public static final byte[] MAGIC = {82, 101, 100, 32, 69, 108, 109, 10}; // "Red Elm\n"
  public static final int CURRENT_VERSION = 1;
  private static final Log LOG = Log.getLog(RedelmFileWriter.class);

  private final CompressionCodec codec;
  private Compressor compressor;
  private CompressionOutputStream cos;

  private final MessageType schema;
  private final FSDataOutputStream out;
  private BlockMetaData currentBlock;
  private ColumnMetaData currentColumn;
  private int currentRecordCount;
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

  private long uncompressedLength;

  /**
   * Captures the order in which methods should be called
   *
   * @author Julien Le Dem
   *
   */
  private enum STATE {
    NOT_STARTED {
      STATE start() {
        return STARTED;
      }
    },
    STARTED {
      STATE startBlock() {
        return BLOCK;
      }
      STATE end() {
        return ENDED;
      }
    },
    BLOCK  {
      STATE startColumn() {
        return COLUMN;
      }
      STATE endBlock() {
        return STARTED;
      }
    },
    COLUMN {
      STATE startRepetition() {
        return REPETITION;
      }
    },
    REPETITION {
      STATE startDefinition() {
        return DEFINITION;
      }
      STATE write() {
        return this;
      }
    },
    DEFINITION {
      STATE startData() {
        return DATA;
      }
      STATE write() {
        return this;
      }
    },
    DATA {
      STATE endColumn() {
        return BLOCK;
      };
      STATE write() {
        return this;
      }
    },
    ENDED;

    STATE start() {throw new IllegalStateException(this.name());}
    STATE startBlock() {throw new IllegalStateException(this.name());}
    STATE startColumn() {throw new IllegalStateException(this.name());}
    STATE startRepetition() {throw new IllegalStateException(this.name());}
    STATE startDefinition() {throw new IllegalStateException(this.name());}
    STATE startData() {throw new IllegalStateException(this.name());}
    STATE endColumn() {throw new IllegalStateException(this.name());}
    STATE endBlock() {throw new IllegalStateException(this.name());}
    STATE end() {throw new IllegalStateException(this.name());}
    STATE write() {throw new IllegalStateException(this.name());}
  }

  private STATE state = STATE.NOT_STARTED;

  /**
   *
   * @param schema the schema of the data
   * @param out the file to write to
   * @param codec the codec to use to compress blocks
   */
  public RedelmFileWriter(MessageType schema, FSDataOutputStream out, CompressionCodec codec) {
    super();
    this.schema = schema;
    this.out = out;
    this.codec = codec;
  }

  /**
   * start the file
   * @throws IOException
   */
  public void start() throws IOException {
    state = state.start();
    out.write(MAGIC);
  }

  /**
   * start a block
   * @param recordCount the record count in this block
   * @throws IOException
   */
  public void startBlock(int recordCount) throws IOException {
    state = state.startBlock();
    currentBlock = new BlockMetaData(out.getPos());
    currentRecordCount = recordCount;
  }

  /**
   * start a column inside a block
   * @param descriptor the column descriptor
   * @param valueCount the value count in this column
   * @throws IOException
   */
  public void startColumn(ColumnDescriptor descriptor, int valueCount) throws IOException {
    state = state.startColumn();
    currentColumn = new ColumnMetaData(descriptor.getPath(), descriptor.getType());
    currentColumn.setValueCount(valueCount);
  }

  private void startCompression() throws IOException {
    if (codec != null) {
      compressor = CodecPool.getCompressor(codec);
      cos = codec.createOutputStream(out, compressor);
      uncompressedLength = 0;
    }
  }

  private void endCompression() throws IOException {
    if (codec != null) {
      cos.finish();
      cos = null;
      CodecPool.returnCompressor(compressor);
      compressor = null;
    }
  }

  /**
   * starts the repetitionLevels in this column
   * @throws IOException
   */
  public void startRepetitionLevels() throws IOException {
    state = state.startRepetition();
    currentColumn.setRepetitionStart(out.getPos());
    startCompression();
  }

  /**
   * ends the repetition levels and starts the definition levels in this column
   * @throws IOException
   */
  public void startDefinitionLevels() throws IOException {
    state = state.startDefinition();
    endCompression();
    currentColumn.setRepetitionUncompressedLength(uncompressedLength);
    currentColumn.setDefinitionStart(out.getPos());
    startCompression();
  }

  /**
   * ends the definition levels and starts the data in this column
   * @throws IOException
   */
  public void startData() throws IOException {
    state = state.startData();
    endCompression();
    currentColumn.setDefinitionUncompressedLength(uncompressedLength);
    currentColumn.setDataStart(out.getPos());
    startCompression();
  }

  /**
   * write binary representation of repetition, definition or data
   * @param data array containing the data
   * @param offset where to start reading from the data
   * @param length how many bytes to read
   */
  public void write(byte[] data, int offset, int length) throws IOException {
    state = state.write();
    if (codec == null) {
      out.write(data, offset, length);
    } else {
      cos.write(data, offset, length);
    }
    uncompressedLength += length;
  }

  /**
   * end a column (once all rep, def and data have been written)
   * @throws IOException
   */
  public void endColumn() throws IOException {
    state = state.endColumn();
    endCompression();
    currentColumn.setDataUncompressedLength(uncompressedLength);
    currentColumn.setDataEnd(out.getPos());
    currentBlock.addColumn(currentColumn);
    if (INFO) LOG.info(currentColumn);
    currentColumn = null;
  }

  /**
   * ends a block once all column have been written
   * @throws IOException
   */
  public void endBlock() throws IOException {
    state = state.endBlock();
    currentBlock.setEndIndex(out.getPos());
    currentBlock.setRecordCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
  }

  /**
   * ends a file once all blocks have been written
   * @param metaDataBlocks the extra meta data blocks to write in the footer
   * @throws IOException
   */
  public void end(List<MetaDataBlock> metaDataBlocks) throws IOException {
    state = state.end();
    long footerIndex = out.getPos();
    out.writeInt(CURRENT_VERSION);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RedelmMetaData footer = new RedelmMetaData(new RedelmMetaData.FileMetaData(schema.toString(), codec == null ? null : codec.getClass().getName()), blocks);
//    out.writeUTF(Footer.toJSON(footer));
    // lazy: use serialization
    // TODO: change that
    new ObjectOutputStream(baos).writeObject(footer);
    MetaDataBlock redelmFooter = new MetaDataBlock("RedElm", baos.toByteArray());
    if (metaDataBlocks.size() > 254) {
      throw new IllegalArgumentException("maximum of 255 metadata blocks in footer");
    }
    out.write(metaDataBlocks.size() + 1);
    writeMetaDataBlock(redelmFooter);
    for (MetaDataBlock metaDataBlock : metaDataBlocks) {
      writeMetaDataBlock(metaDataBlock);
    }
    out.writeLong(footerIndex);
    out.write(MAGIC);
    out.close();
  }

  private void writeMetaDataBlock(MetaDataBlock metaDataBlock) throws IOException {
    out.writeUTF(metaDataBlock.getName());
    out.writeInt(metaDataBlock.getData().length);
    out.write(metaDataBlock.getData());
  }

  public static void writeSummaryFile(Configuration configuration, Path outputPath, List<Footer> footers) throws IOException {
    Path summaryPath = new Path(outputPath, RED_ELM_SUMMARY);
    FileSystem fs = outputPath.getFileSystem(configuration);
    FSDataOutputStream summary = fs.create(summaryPath);
    summary.writeInt(footers.size());
    for (Footer footer : footers) {
      summary.writeUTF(footer.getFile().toString());
      summary.write(footer.getMetaDataBlocks().size());
      for (MetaDataBlock metaDataBlock : footer.getMetaDataBlocks()) {
        summary.writeUTF(metaDataBlock.getName());
        summary.writeInt(metaDataBlock.getData().length);
        summary.write(metaDataBlock.getData());
      }
    }
    summary.close();
  }

}
