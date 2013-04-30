package parquet.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReader;
import parquet.column.page.mem.MemPageStore;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hive.writable.BinaryWritable;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;

public class UtilitiesTestMethods {

  public static void writeToFile(final Path file, final Configuration configuration, final MessageType schema, final MemPageStore pageStore, final int recordCount)
          throws IOException {
    final ParquetFileWriter w = startFile(file, configuration, schema);
    writeBlock(schema, pageStore, recordCount, w);
    endFile(w);
  }

  public static void endFile(final ParquetFileWriter w) throws IOException {
    w.end(new HashMap<String, String>());
  }

  static public MapWritable createMap(final Integer custkey, final String name, final String address, final Integer nationkey, final String phone, final Double acctbal, final String mktsegment, final String comment) {
    final MapWritable map = new MapWritable();

    map.put(new Text("c_custkey"), new IntWritable(custkey));
    if (name != null) {
      map.put(new Text("c_name"), new BinaryWritable(name));
    }
    if (address != null) {
      map.put(new Text("c_address"), new BinaryWritable(address));
    }
    if (nationkey != null) {
      map.put(new Text("c_nationkey"), new IntWritable(nationkey));
    }
    if (phone != null) {
      map.put(new Text("c_phone"), new BinaryWritable(phone));
    }
    if (acctbal != null) {
      map.put(new Text("c_acctbal"), new DoubleWritable(acctbal));
    }
    if (mktsegment != null) {
      map.put(new Text("c_mktsegment"), new BinaryWritable(mktsegment));
    }
    if (comment != null) {
      map.put(new Text("c_comment"), new BinaryWritable(comment));
    }

    return map;
  }
//  public static void readTestFile(Path testFile, Configuration configuration)
//          throws IOException {
//    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, testFile);
//    MessageType schema = readFooter.getFileMetaData().getSchema();
//    MessageType requestedSchema = new MessageType("requested", schema.getFields());
//    ParquetFileReader parquetFileReader = new ParquetFileReader(configuration, testFile, readFooter.getBlocks(), requestedSchema.getColumns());
//    PageReadStore pages = parquetFileReader.readNextRowGroup();
//    List<ColumnDescriptor> columns = requestedSchema.getColumns();
//    for (ColumnDescriptor columnDescriptor : columns) {
//      PageReader pageReader = pages.getPageReader(columnDescriptor);
//      Page page = null;
//      do {
//        page = pageReader.readPage();
//        if (page != null) {
//          System.out.print("data number element:" + page.getValueCount());
//          System.out.print("data number getValueEncoding:" + page.getValueEncoding());
//          String data = new String(page.getBytes().toByteArray(), BytesUtils.UTF8);
//          System.out.print("data:" + data);
//        }
//        System.out.println();
//      } while (page != null);
//    }
//
//    System.out.println("pages : " + pages);
////    System.out.println(pages.getRowCount());
//  }

  public static void writeBlock(final MessageType schema, final MemPageStore pageStore,
          final int recordCount, final ParquetFileWriter w) throws IOException {
    w.startBlock(recordCount);
    final List<ColumnDescriptor> columns = schema.getColumns();
    for (final ColumnDescriptor columnDescriptor : columns) {
      final PageReader pageReader = pageStore.getPageReader(columnDescriptor);
      final long totalValueCount = pageReader.getTotalValueCount();
      w.startColumn(columnDescriptor, totalValueCount, CompressionCodecName.UNCOMPRESSED);
      int n = 0;
      do {
        final Page page = pageReader.readPage();
        n += page.getValueCount();
        // TODO: change INTFC
        w.writeDataPage(
                page.getValueCount(),
                (int) page.getBytes().size(),
                BytesInput.from(page.getBytes().toByteArray()),
                page.getRlEncoding(),
                page.getDlEncoding(),
                page.getValueEncoding());
      } while (n < totalValueCount);
      w.endColumn();
    }
    w.endBlock();
  }

  public static ParquetFileWriter startFile(final Path file,
          final Configuration configuration, final MessageType schema) throws IOException {
    final ParquetFileWriter w = new ParquetFileWriter(configuration, schema, file);
    w.start();
    return w;
  }

  public static boolean mapEquals(final MapWritable first, final MapWritable second) {

    if (first == second) {
      return true;
    }

    if (second instanceof MapWritable) {
      if (first.size() != second.size()) {
        return false;
      }
      return first.entrySet().equals(second.entrySet());
    }
    return false;
  }

  public static void writeField(final RecordConsumer recordWriter, final int index, final String name, final Object value) {
    if (value != null) {
      recordWriter.startField(name, index);
      if (value instanceof Integer) {
        recordWriter.addInteger((Integer) value);
      } else if (value instanceof String) {
        recordWriter.addBinary(Binary.fromString((String) value));
      } else if (value instanceof Double) {
        recordWriter.addDouble((Double) value);
      } else {
        throw new IllegalArgumentException(value.getClass().getName() + " not supported");
      }

      recordWriter.endField(name, index);
    }
  }
}
