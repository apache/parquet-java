package redelm.pig;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import redelm.Log;
import redelm.bytes.BytesInput;
import redelm.column.ColumnDescriptor;
import redelm.column.mem.MemColumnWriteStore;
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
import redelm.schema.PrimitiveType.PrimitiveTypeName;
import redelm.schema.Type.Repetition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class GenerateIntTestFile {
  private static final Log LOG = Log.getLog(GenerateIntTestFile.class);

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
      MessageType schema = new MessageType("int_test_file", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "int_col"));

      MemPageStore pageStore = new MemPageStore();
      MemColumnWriteStore store = new MemColumnWriteStore(pageStore, 8*1024);
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
      store.flush();


      writeToFile(testFile, configuration, schema, pageStore, recordCount);
    }

    {
      readTestFile(testFile, configuration);
    }
  }

  public static void readTestFile(Path testFile, Configuration configuration)
      throws IOException {
    RedelmMetaData readFooter = RedelmFileReader.readFooter(configuration, testFile);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    RedelmFileReader redelmFileReader = new RedelmFileReader(configuration, testFile, readFooter.getBlocks(), schema.getPaths());
    redelmFileReader.readColumns(new PageConsumer() {
      @Override
      public void consumePage(String[] path, int valueCount, BytesInput bytes) {
        if (Log.INFO) LOG.info(Arrays.toString(path) + " " + valueCount + " " + bytes.size());
        try {
          BytesInput.copy(bytes);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  public static void writeToFile(Path file, Configuration configuration, MessageType schema, MemPageStore pageStore, int recordCount)
      throws IOException {
    RedelmFileWriter w = startFile(file, configuration, schema);
    writeBlock(schema, pageStore, recordCount, w);
    endFile(w);
  }

  public static void endFile(RedelmFileWriter w) throws IOException {
    w.end(new HashMap<String, String>());
  }

  public static void writeBlock(MessageType schema, MemPageStore pageStore,
      int recordCount, RedelmFileWriter w) throws IOException {
    w.startBlock(recordCount);
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
        w.writeDataPage(page.getValueCount(), (int)page.getBytes().size(), BytesInput.from(page.getBytes().toByteArray()));
      } while (n < totalValueCount);
      w.endColumn();
    }
    w.endBlock();
  }

  public static RedelmFileWriter startFile(Path file,
      Configuration configuration, MessageType schema) throws IOException {
    RedelmFileWriter w = new RedelmFileWriter(configuration, schema, file);
    w.start();
    return w;
  }
}
