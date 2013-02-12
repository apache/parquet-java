package redelm.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import redelm.bytes.BytesInput;
import redelm.column.ColumnDescriptor;
import redelm.column.mem.PageReader;
import redelm.column.mem.PageStore;
import redelm.column.mem.PageWriter;
import redelm.hadoop.CodecFactory.BytesCompressor;
import redelm.redfile.RedFileMetadataConverter;
import redelm.schema.MessageType;
import redfile.DataPageHeader;
import redfile.Encoding;
import redfile.PageHeader;
import redfile.PageType;

public class ColumnChunkPageStore extends PageStore {

  private static RedFileMetadataConverter redFileMetadataConverter = new RedFileMetadataConverter();

  private static final class ColumnChunkPageWriter implements PageWriter {
    private ByteArrayOutputStream buf = new ByteArrayOutputStream(1024*1024/2);
    private long uncompressedLength;
    private long compressedLength;
    private final BytesCompressor compressor;

    private ColumnChunkPageWriter(BytesCompressor compressor) {
      this.compressor = compressor;
    }

    @Override
    public void writePage(BytesInput bytes, int valueCount)
        throws IOException {
      long uncompressedSize = bytes.size();
      BytesInput compressedBytes = compressor.compress(bytes);
      long compressedSize = compressedBytes.size();
      PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, (int)uncompressedSize, (int)compressedSize);
      // pageHeader.crc = ...;
      pageHeader.data_page = new DataPageHeader(valueCount, Encoding.PLAIN); // TODO: encoding
      redFileMetadataConverter.writePageHeader(pageHeader, buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      compressedBytes.writeAllTo(buf);
    }

    @Override
    public long getMemSize() {
      return buf.size();
    }

    public void writeToFileWriter(RedelmFileWriter writer) throws IOException {
      writer.writeDataPages(BytesInput.from(buf), uncompressedLength, compressedLength);
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageWriter> buffers = new HashMap<ColumnDescriptor, ColumnChunkPageWriter>();
  private final MessageType schema;
  private final BytesCompressor compressor;

  public ColumnChunkPageStore(BytesCompressor compressor, MessageType schema) {
    this.compressor = compressor;
    this.schema = schema;
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    if (!buffers.containsKey(path)) {
      buffers.put(path,  new ColumnChunkPageWriter(compressor));
    }
    return buffers.get(path);
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor descriptor) {
    throw new UnsupportedOperationException("Write only store");
  }

  public void flushToFileWriter(RedelmFileWriter writer) throws IOException {
    List<ColumnDescriptor> columns = schema.getColumns();
    for (ColumnDescriptor columnDescriptor : columns) {
      ColumnChunkPageWriter pageWriter = buffers.get(columnDescriptor);
      pageWriter.writeToFileWriter(writer);
    }
  }

}
