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
import parquet.data.materializer.GroupWriterImpl;
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
    log(schema);
    log("r1");
    log(r1);
    log("r2");
    log(r2);

    ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
    List<Group> records = new ArrayList<Group>();
    MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
    // generate compiled recordConverter
    RecordMaterializer<Group> recordConverter = new GroupMaterializer(schema);
    {
      // write example records
      MemPageStore memPageStore = new MemPageStore(2);
      ColumnWriteStoreImpl columns = new ColumnWriteStoreImpl(memPageStore, 800, 800, 800, false, WriterVersion.PARQUET_1_0);
      log(columnIO);
      GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), schema);
      groupWriter.write(r1);
      groupWriter.write(r2);
      columns.flush();
      log(columns);
      log("=========");


      RecordReader<Group> recordReader = columnIO.getRecordReader(memPageStore, recordConverter);

      // read records with compiled groups
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

    {
      // write the compiled groups
      MemPageStore memPageStore = new MemPageStore(records.size());
      ColumnWriteStoreImpl columns = new ColumnWriteStoreImpl(memPageStore, 800, 800, 800, false, WriterVersion.PARQUET_1_0);
      parquet.data.GroupWriter groupWriter = GroupWriterImpl.newGroupWriter(columnIO.getRecordWriter(columns), schema);
      for (Group group : records) {
        groupWriter.write(group);
      }
      columns.flush();
      RecordReader<Group> recordReader2 = columnIO.getRecordReader(memPageStore, recordConverter);
      // read compiled groups back and check for correctness.
      List<Group> records2 = new ArrayList<Group>();
      records2.add(recordReader2.read());
      records2.add(recordReader2.read());
      assertEquals("deserialization does not display the same result", r1.toString(), records2.get(0).toString());
      assertEquals("deserialization does not display the same result", r2.toString(), records2.get(1).toString());

    }
  }
}
