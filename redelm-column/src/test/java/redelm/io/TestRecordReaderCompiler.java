package redelm.io;

import static redelm.data.simple.example.Paper.r1;
import static redelm.data.simple.example.Paper.schema;
import static redelm.io.TestColumnIO.expectedEventsForR1;

import java.util.ArrayDeque;
import java.util.Deque;

import redelm.column.ColumnsStore;
import redelm.column.mem.MemColumnsStore;
import redelm.data.GroupWriter;

import org.junit.Test;

public class TestRecordReaderCompiler {

  @Test
  public void testRecordReaderCompiler() {

      ColumnsStore columns = new MemColumnsStore(1024, schema);
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema, columns);
      new GroupWriter(columnIO.getRecordWriter(), schema).write(r1);
      columns.flip();
      RecordReader recordReader = new RecordReaderCompiler().compile(columnIO.getRecordReader());

      final Deque<String> expectations = new ArrayDeque<String>();
      for (String string : expectedEventsForR1) {
        expectations.add(string);
      }

      recordReader.read(new ExpectationValidatingRecordConsumer(expectations));

    }
}
