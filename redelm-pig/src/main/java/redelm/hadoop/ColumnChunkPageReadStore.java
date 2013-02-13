package redelm.hadoop;

import static redelm.Log.DEBUG;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import redelm.Log;
import redelm.bytes.BytesInput;
import redelm.column.ColumnDescriptor;
import redelm.column.mem.Page;
import redelm.column.mem.PageReader;
import redelm.column.mem.PageReadStore;
import redelm.column.mem.PageWriter;
import redelm.hadoop.CodecFactory.BytesDecompressor;
import redelm.redfile.RedFileMetadataConverter;
import redfile.PageHeader;
import redfile.PageType;

public class ColumnChunkPageReadStore implements PageReadStore {
  private static final Log LOG = Log.getLog(ColumnChunkPageReadStore.class);

  private static RedFileMetadataConverter redFileMetadataConverter = new RedFileMetadataConverter();

  private static final class ColumnChunkPageReader implements PageReader {

    private final BytesDecompressor decompressor;
    private final InputStream bytes;
    private final int valueCount;

    public ColumnChunkPageReader(BytesDecompressor decompressor, InputStream bytes, int valueCount) {
      this.decompressor = decompressor;
      this.bytes = bytes;
      this.valueCount = valueCount;
    }

    @Override
    public int getTotalValueCount() {
      return valueCount;
    }

    @Override
    public Page readPage() {
      try {
        PageHeader pageHeader = readNextDataPageHeader();
        return new Page(
            decompressor.decompress(BytesInput.from(bytes, pageHeader.compressed_page_size), pageHeader.uncompressed_page_size),
            pageHeader.data_page.num_values);
      } catch (IOException e) {
        throw new RuntimeException(e); // TODO: cleanup
      }
    }

    private PageHeader readNextDataPageHeader() throws IOException {
      PageHeader pageHeader;
      do {
        if (DEBUG) LOG.debug("reading page");
        try {
          pageHeader = redFileMetadataConverter.readPageHeader(bytes);
          if (pageHeader.type != PageType.DATA_PAGE) {
            if (DEBUG) LOG.debug("not a data page, skipping " + pageHeader.compressed_page_size);
            bytes.skip(pageHeader.compressed_page_size);
          }
        } catch (IOException e) {
          throw new IOException("could not read page header", e);
        }
      } while (pageHeader.type != PageType.DATA_PAGE);
      return pageHeader;
    }

  }

  private final Map<ColumnDescriptor, ColumnChunkPageReader> readers = new HashMap<ColumnDescriptor, ColumnChunkPageReader>();
  private final BytesDecompressor decompressor;

  public ColumnChunkPageReadStore(BytesDecompressor decompressor) {
    this.decompressor = decompressor;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor path) {
    if (!readers.containsKey(path)) {
      throw new IllegalArgumentException(path + " is not in the store");
    }
    return readers.get(path);
  }

  public void addColumn(ColumnDescriptor path, InputStream bytes, int valueCount) {
    readers.put(path, new ColumnChunkPageReader(decompressor, bytes, valueCount));
  }

}
