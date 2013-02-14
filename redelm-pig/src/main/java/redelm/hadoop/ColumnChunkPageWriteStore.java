package redelm.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redelm.bytes.BytesInput;
import redelm.column.ColumnDescriptor;
import redelm.column.mem.PageReader;
import redelm.column.mem.PageWriteStore;
import redelm.column.mem.PageWriter;
import redelm.hadoop.CodecFactory.BytesCompressor;
import redelm.redfile.RedFileMetadataConverter;
import redelm.schema.MessageType;
import redfile.DataPageHeader;
import redfile.Encoding;
import redfile.PageHeader;
import redfile.PageType;

public class ColumnChunkPageWriteStore implements PageWriteStore {

  private static RedFileMetadataConverter redFileMetadataConverter = new RedFileMetadataConverter();

  private static final class ColumnChunkPageWriter implements PageWriter {
    private ByteArrayOutputStream buf = new ByteArrayOutputStream(1024*1024/2);
    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;
    private final BytesCompressor compressor;
    private final ColumnDescriptor path;

    private ColumnChunkPageWriter(ColumnDescriptor path, BytesCompressor compressor) {
      this.path = path;
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
      this.totalValueCount += valueCount;
      compressedBytes.writeAllTo(buf);
    }

    @Override
    public long getMemSize() {
      return buf.size();
    }

    public void writeToFileWriter(RedelmFileWriter writer) throws IOException {
      writer.startColumn(path, totalValueCount, compressor.getCodecName());
      writer.writeDataPages(BytesInput.from(buf), uncompressedLength, compressedLength);
      writer.endColumn();
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageWriter> writers = new HashMap<ColumnDescriptor, ColumnChunkPageWriter>();
  private final MessageType schema;
  private final BytesCompressor compressor;

  public ColumnChunkPageWriteStore(BytesCompressor compressor, MessageType schema) {
    this.compressor = compressor;
    this.schema = schema;
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    if (!writers.containsKey(path)) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor));
    }
    return writers.get(path);
  }

  public void flushToFileWriter(RedelmFileWriter writer) throws IOException {
    List<ColumnDescriptor> columns = schema.getColumns();
    for (ColumnDescriptor columnDescriptor : columns) {
      ColumnChunkPageWriter pageWriter = writers.get(columnDescriptor);
      pageWriter.writeToFileWriter(writer);
    }
  }

}
