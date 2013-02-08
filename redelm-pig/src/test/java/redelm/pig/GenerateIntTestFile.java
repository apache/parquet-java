package redelm.pig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import redelm.column.ColumnDescriptor;
import redelm.column.mem.MemColumnsStore;
import redelm.column.mem.MemPageStore;
import redelm.column.mem.Page;
import redelm.column.mem.PageReader;
import redelm.hadoop.PageConsumer;
import redelm.hadoop.RedelmFileReader;
import redelm.hadoop.RedelmFileWriter;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.hadoop.metadata.RedelmMetaData;
import redelm.io.ColumnIOFactory;
import redelm.io.MessageColumnIO;
import redelm.io.RecordConsumer;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;
import redelm.schema.PrimitiveType.Primitive;
import redelm.schema.Type.Repetition;

public class GenerateIntTestFile {
  public static void main(String[] args) throws Throwable {
    File out = new File("testdata/from_java/int_test_file");
    if (out.exists()) {
      if (!out.delete()) {
        throw new RuntimeException("can not remove existing file " + out.getAbsolutePath());
      }
    }
    Path testFile = new Path(out.toURI());
    Configuration configuration = new Configuration();
    {
      MessageType schema = new MessageType("int_test_file", new PrimitiveType(Repetition.OPTIONAL, Primitive.INT32, "int_col"));

      RedelmFileWriter w = new RedelmFileWriter(configuration, schema, testFile);


      MemPageStore pageStore = new MemPageStore();
      MemColumnsStore store = new MemColumnsStore(1024 * 1024 * 1, pageStore, 8*1024);
      //
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

      RecordConsumer recordWriter = columnIO.getRecordWriter(store);

      int recordCount = 0;
      for (int i = 0; i < 100; i++) {
        recordWriter.startMessage();
        recordWriter.startField("int_col", 0);
        if (i % 10 != 0) {
          recordWriter.addInteger(i);
        }
        recordWriter.endField("int_col", 0);
        recordWriter.endMessage();
        ++ recordCount;
      }


      w.start();
      w.startBlock(recordCount);
      store.flush();
      List<ColumnDescriptor> columns = schema.getColumns();
      for (ColumnDescriptor columnDescriptor : columns) {
        PageReader pageReader = pageStore.getPageReader(columnDescriptor);
        int totalValueCount = pageReader.getTotalValueCount();
        w.startColumn(columnDescriptor, totalValueCount, CompressionCodecName.UNCOMPRESSED);
        int n = 0;
        do {
          Page page = pageReader.readPage();
          n += page.getValueCount();
          // TODO: change INTFC
          w.writeDataPage(page.getValueCount(), (int)page.getBytes().size(), page.getBytes().toByteArray(), 0, (int)page.getBytes().size());
        } while (n < totalValueCount);
        w.endColumn();
      }
      recordCount = 0;
      w.endBlock();
      store = null;
      pageStore = null;
      w.end(new HashMap<String, String>());
    }

    {
      RedelmMetaData readFooter = RedelmFileReader.readFooter(configuration, testFile);
      MessageType schema = readFooter.getFileMetaData().getSchema();
      RedelmFileReader redelmFileReader = new RedelmFileReader(configuration, testFile, readFooter.getBlocks(), schema.getPaths());
      redelmFileReader.readColumns(new PageConsumer() {
        @Override
        public void consumePage(String[] path, int valueCount, InputStream is,
            int pageSize) {
          System.out.println(Arrays.toString(path) + " " + valueCount + " " + pageSize);
        }
      });
    }
  }
}
