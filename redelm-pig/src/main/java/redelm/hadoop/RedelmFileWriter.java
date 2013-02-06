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

import static redelm.Log.DEBUG;
import static redelm.Log.INFO;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import redelm.Log;
import redelm.bytes.BytesUtils;
import redelm.column.ColumnDescriptor;
import redelm.hadoop.metadata.BlockMetaData;
import redelm.hadoop.metadata.ColumnChunkMetaData;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.hadoop.metadata.FileMetaData;
import redelm.hadoop.metadata.RedelmMetaData;
import redelm.redfile.RedFileMetadataConverter;
import redelm.schema.MessageType;
import redfile.DataPageHeader;
import redfile.Encoding;
import redfile.PageHeader;
import redfile.PageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Writes a RedElm file
 * @author Julien Le Dem
 *
 */
public class RedelmFileWriter {
  private static final Log LOG = Log.getLog(RedelmFileWriter.class);

  public static final String RED_ELM_SUMMARY = "_RedElmSummary";
//  public static final byte[] MAGIC = {82, 101, 100, 32, 69, 108, 109, 10}; // "Red Elm\n"
  public static final byte[] MAGIC = {82, 69, 68, 49}; // "RED1"
  public static final int CURRENT_VERSION = 1;

  private static RedFileMetadataConverter redFileMetadataConverter = new RedFileMetadataConverter();

  private final MessageType schema;
  private final FSDataOutputStream out;
  private BlockMetaData currentBlock;
  private ColumnChunkMetaData currentColumn;
  private int currentRecordCount;
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
  private long uncompressedLength;
  private final RedFileMetadataConverter metadataConverter = new RedFileMetadataConverter();

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
    STATE write() {throw new IllegalStateException(this.name());}
    STATE endColumn() {throw new IllegalStateException(this.name());}
    STATE endBlock() {throw new IllegalStateException(this.name());}
    STATE end() {throw new IllegalStateException(this.name());}
  }

  private STATE state = STATE.NOT_STARTED;

  /**
   *
   * @param schema the schema of the data
   * @param out the file to write to
   * @param codec the codec to use to compress blocks
   * @throws IOException if the file can not be created
   */
  public RedelmFileWriter(Configuration configuration, MessageType schema, Path file) throws IOException {
    super();
    this.schema = schema;
    FileSystem fs = file.getFileSystem(configuration);
    this.out = fs.create(file, false);
  }

  /**
   * start the file
   * @throws IOException
   */
  public void start() throws IOException {
    state = state.start();
    if (DEBUG) LOG.debug(out.getPos() + ": start");
    out.write(MAGIC);
  }

  /**
   * start a block
   * @param recordCount the record count in this block
   * @throws IOException
   */
  public void startBlock(int recordCount) throws IOException {
    state = state.startBlock();
    if (DEBUG) LOG.debug(out.getPos() + ": start block");
    out.write(MAGIC);
    currentBlock = new BlockMetaData();
    currentRecordCount = recordCount;
  }

  /**
   * start a column inside a block
   * @param descriptor the column descriptor
   * @param valueCount the value count in this column
   * @param compressionCodecName
   * @throws IOException
   */
  public void startColumn(ColumnDescriptor descriptor, int valueCount, CompressionCodecName compressionCodecName) throws IOException {
    state = state.startColumn();
    if (DEBUG) LOG.debug(out.getPos() + ": start column: " + descriptor + " count=" + valueCount);
    currentColumn = new ColumnChunkMetaData(descriptor.getPath(), descriptor.getType(), compressionCodecName);
    currentColumn.setValueCount(valueCount);
    currentColumn.setFirstDataPage(out.getPos());
  }

  /**
   * write binary representation of repetition, definition or data
   * @param data array containing the data
   * @param offset where to start reading from the data
   * @param length how many bytes to read
   */
  public void writeDataPage(
      int valueCount, int uncompressedPageSize,
      byte[] bytes, int offset, int length) throws IOException {
    state = state.write();
    if (DEBUG) LOG.debug(out.getPos() + ": write data page: " + valueCount + " values");
//      codec = codecFactory.getCodec(currentColumn.getCodec());
//      compressor = CodecPool.getCompressor(codec);
//      cos = codec.createOutputStream(compressedOut, compressor);
//      uncompressedLength = 0;
//      cos.write(data, offset, length);
//      cos.finish();
//      cos = null;
//      CodecPool.returnCompressor(compressor);
//      compressor = null;
//      codec = null;
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedPageSize, length);
    // pageHeader.crc = ...;
    pageHeader.data_page = new DataPageHeader(valueCount, Encoding.PLAIN); // TODO: encoding
    metadataConverter.writePageHeader(pageHeader, out);
    this.uncompressedLength += uncompressedPageSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write data page content "+length);
    out.write(bytes, offset, length);
  }

  /**
   * end a column (once all rep, def and data have been written)
   * @throws IOException
   */
  public void endColumn() throws IOException {
    state = state.endColumn();
    if (DEBUG) LOG.debug(out.getPos() + ": end column");
    currentColumn.setTotalUncompressedSize(uncompressedLength);
    currentBlock.addColumn(currentColumn);
    if (INFO) LOG.info(currentColumn);
    currentColumn = null;
  }

  /**
   * ends a block once all column chunks have been written
   * @throws IOException
   */
  public void endBlock() throws IOException {
    state = state.endBlock();
    if (DEBUG) LOG.debug(out.getPos() + ": end block");
    currentBlock.setRowCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
  }

  /**
   * ends a file once all blocks have been written.
   * closes the file.
   * @param extraMetaData the extra meta data to write in the footer
   * @throws IOException
   */
  public void end(Map<String, String> extraMetaData) throws IOException {
    state = state.end();
    if (DEBUG) LOG.debug(out.getPos() + ": end");
    long footerIndex = out.getPos();
    RedelmMetaData footer = new RedelmMetaData(new FileMetaData(schema), blocks, extraMetaData);
    serializeFooter(footer, out);
    if (DEBUG) LOG.debug(out.getPos() + ": footer length = " + (out.getPos() - footerIndex));
    BytesUtils.writeIntLittleEndian(out, (int)(out.getPos() - footerIndex));
    out.write(MAGIC);
    out.close();
  }

  private void serializeFooter(RedelmMetaData footer, OutputStream os) throws IOException {
    redfile.FileMetaData redFileMetadata = new RedFileMetadataConverter().toRedFileMetadata(CURRENT_VERSION, footer);
    metadataConverter.writeFileMetaData(redFileMetadata, os);
  }

  public static void writeSummaryFile(Configuration configuration, Path outputPath, List<Footer> footers) throws IOException {
    Path summaryPath = new Path(outputPath, RED_ELM_SUMMARY);
    FileSystem fs = outputPath.getFileSystem(configuration);
    FSDataOutputStream summary = fs.create(summaryPath);
    summary.writeInt(footers.size());
    for (Footer footer : footers) {
      summary.writeUTF(footer.getFile().toString());
      redfile.FileMetaData redFileMetadata = redFileMetadataConverter.toRedFileMetadata(CURRENT_VERSION, footer.getRedelmMetaData());
      redFileMetadataConverter.writeFileMetaData(redFileMetadata, summary);
    }
    summary.close();
  }

}
