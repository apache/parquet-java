package redelm.io;

import static redelm.data.simple.example.Paper.r1;
import static redelm.data.simple.example.Paper.schema;
import static redelm.io.TestColumnIO.expectedEventsForR1;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import redelm.column.ColumnsStore;
import redelm.column.mem.MemColumnsStore;
import redelm.data.GroupWriter;

import org.junit.Test;

public class TestRecordReaderCompiler {

  @Test
  public void testRecordReaderCompiler() {


    Logger.getLogger("brennus").setLevel(Level.FINEST);
    Logger.getLogger("brennus").addHandler(new Handler() {
      public void publish(LogRecord record) {
        System.err.println(record.getMessage());
      }
      public void flush() {
        System.err.flush();
      }
      public void close() throws SecurityException {
        System.err.flush();
      }
    });

      ColumnsStore columns = new MemColumnsStore(1024, schema);
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      new GroupWriter(columnIO.getRecordWriter(columns), schema).write(r1);
      columns.flip();
      System.err.flush();
      Logger.getLogger("brennus").info("compile");
      System.out.println("compile");
      RecordReader<Void> recordReader = columnIO.getRecordReader(
          columns,
          new ExpectationValidatingRecordConsumer(
              new ArrayDeque<String>(Arrays.asList(expectedEventsForR1))));
      recordReader = new RecordReaderCompiler().compile((RecordReaderImplementation<Void>)recordReader);

      Logger.getLogger("brennus").info("read");
      System.out.println("read");
      recordReader.read();

    }
}
