package redelm.io;

import static redelm.data.simple.example.Paper.r1;
import static redelm.data.simple.example.Paper.schema;
import static redelm.io.TestColumnIO.expectedEventsForR1;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import redelm.column.ColumnsStore;
import redelm.column.mem.MemColumnsStore;
import redelm.data.GroupWriter;

import org.junit.Test;

public class TestRecordReaderCompiler {

//  @Test
  public void testRecordReaderCompiler() {


    Logger.getLogger("brennus").setLevel(Level.FINEST);
    Logger.getLogger("brennus").addHandler(new Handler() {
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

      ColumnsStore columns = new MemColumnsStore(1024, schema);
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      new GroupWriter(columnIO.getRecordWriter(columns), schema).write(r1);
      columns.flip();
      RecordReader recordReader = new RecordReaderCompiler().compile(
          columnIO.getRecordReader(
              columns,
              new ExpectationValidatingRecordConsumer(
                  new ArrayDeque<String>(Arrays.asList(expectedEventsForR1)))));

      final Deque<String> expectations = new ArrayDeque<String>();
      for (String string : expectedEventsForR1) {
        expectations.add(string);
      }

      recordReader.read();

    }
}
