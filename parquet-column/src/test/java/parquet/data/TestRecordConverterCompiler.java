package parquet.data;

import static org.junit.Assert.assertEquals;
import static parquet.example.Paper.r1;
import static parquet.example.Paper.r2;
import static parquet.example.Paper.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.Test;

import parquet.Log;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.column.page.mem.MemPageStore;
import parquet.data.materializer.GroupMaterializer;
import parquet.example.data.GroupWriter;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.io.api.RecordMaterializer;

public class TestRecordConverterCompiler {
  private static final Log LOG = Log.getLog(TestRecordConverterCompiler.class);

  private void log(Object o) {
    LOG.info(o);
  }
  @Test
  public void testColumnIO() {
    Logger.getLogger("brennus").setLevel(Level.FINE);
    Logger.getLogger("brennus").addHandler(new Handler() {
      {
        setLevel(Level.FINE);
      }
      public void publish(LogRecord record) {
        System.out.println(record.getMessage());
      }
      public void flush() {
        System.out.flush();
      }
      public void close() throws SecurityException {
        System.out.flush();
      }
    });

    log(schema);
    log("r1");
    log(r1);
    log("r2");
    log(r2);

    MemPageStore memPageStore = new MemPageStore(2);
    ColumnWriteStoreImpl columns = new ColumnWriteStoreImpl(memPageStore, 800, 800, 800, false, WriterVersion.PARQUET_1_0);

    ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
    {
      MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
      log(columnIO);
      GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), schema);
      groupWriter.write(r1);
      groupWriter.write(r2);
      columns.flush();
      log(columns);
      log("=========");

      RecordMaterializer<Group> recordConverter = new GroupMaterializer(schema);

      RecordReader<Group> recordReader = columnIO.getRecordReader(memPageStore, recordConverter);

      List<Group> records = new ArrayList<Group>();
      records.add(recordReader.read());
      records.add(recordReader.read());

      int i = 0;
      for (Group record : records) {
        log("r" + (++i));
        log(record);
      }
      assertEquals("deserialization does not display the same result", r1.toString(), records.get(0).toString());
      assertEquals("deserialization does not display the same result", r2.toString(), records.get(1).toString());
    }
  }
}
