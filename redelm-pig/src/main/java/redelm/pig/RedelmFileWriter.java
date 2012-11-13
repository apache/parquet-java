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
package redelm.pig;

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
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

public class RedelmFileWriter extends BytesOutput {
  public static final byte[] MAGIC = {82, 101, 100, 32, 69, 108, 109, 10}; // "Red Elm\n"
  public static final int CURRENT_VERSION = 1;
  private static final Log LOG = Log.getLog(RedelmFileWriter.class);

  private final String codecClassName;
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

  public RedelmFileWriter(MessageType schema, FSDataOutputStream out, String codecClassName) {
    super();
    this.schema = schema;
    this.out = out;
    this.codecClassName = codecClassName;
    try {
      Class<?> codecClass = Class.forName(codecClassName);
      Configuration conf = new Configuration();
      codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Should not happen", e);
    }
  }

  public void start() throws IOException {
    state = state.start();
    out.write(MAGIC);
  }

  public void startBlock(int recordCount) throws IOException {
    state = state.startBlock();
    currentBlock = new BlockMetaData(out.getPos());
    currentRecordCount = recordCount;
  }

  public void startColumn(ColumnDescriptor descriptor, int valueCount) throws IOException {
    state = state.startColumn();
    currentColumn = new ColumnMetaData(descriptor.getPath(), descriptor.getType());
    currentColumn.setValueCount(valueCount);
  }

  private void startCompression() throws IOException {
    compressor = CodecPool.getCompressor(codec);
    cos = codec.createOutputStream(out, compressor);
    uncompressedLength = 0;
  }

  private void endCompression() throws IOException {
    cos.finish();
    cos = null;
    CodecPool.returnCompressor(compressor);
    compressor = null;
  }

  public void startRepetitionLevels() throws IOException {
    state = state.startRepetition();
    currentColumn.setRepetitionStart(out.getPos());
    startCompression();
  }

  public void startDefinitionLevels() throws IOException {
    state = state.startDefinition();
    endCompression();
    currentColumn.setRepetitionUncompressedLenght(uncompressedLength);
    currentColumn.setDefinitionStart(out.getPos());
    startCompression();
  }

  public void startData() throws IOException {
    state = state.startData();
    endCompression();
    currentColumn.setDefinitionUncompressedLength(uncompressedLength);
    currentColumn.setDataStart(out.getPos());
    startCompression();
  }

  public void write(byte[] data, int offset, int length) throws IOException {
    state = state.write();
    cos.write(data, offset, length);
    uncompressedLength += length;
  }

  public void endColumn() throws IOException {
    state = state.endColumn();
    endCompression();
    currentColumn.setDataUncompressedLength(uncompressedLength);
    currentColumn.setDataEnd(out.getPos());
    currentBlock.addColumn(currentColumn);
    if (INFO) LOG.info(currentColumn);
    currentColumn = null;
  }

  public void endBlock() throws IOException {
    state = state.endBlock();
    currentBlock.setEndIndex(out.getPos());
    currentBlock.setRecordCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
  }

  public void end(List<MetaDataBlock> metaDataBlocks) throws IOException {
    state = state.end();
    long footerIndex = out.getPos();
    out.writeInt(CURRENT_VERSION);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Footer footer = new Footer(schema.toString(), codecClassName, blocks);
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

}
