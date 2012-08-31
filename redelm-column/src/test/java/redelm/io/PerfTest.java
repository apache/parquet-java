package redelm.io;

import java.util.Arrays;

import redelm.column.mem.MemColumnsStore;
import redelm.data.Group;
import redelm.data.simple.SimpleGroupFactory;


public class PerfTest {

  public static void main(String[] args) {
    MemColumnsStore columns = new MemColumnsStore(50*1024*1024);
    {
      MessageColumnIO columnIO = new ColumnIOFactory(new SimpleGroupFactory(TestColumnIO.schema)).getColumnIO(TestColumnIO.schema, columns);

      {
        RecordWriter recordWriter = columnIO.getRecordWriter();
        recordWriter.write(Arrays.<Group>asList(TestColumnIO.r1, TestColumnIO.r2).iterator());

        write(recordWriter, 10000);
        write(recordWriter, 10000);
        write(recordWriter, 10000);
        write(recordWriter, 100000);
        write(recordWriter, 1000000);
      }
      System.out.println("read all");
      {
        RecordReader recordReader = columnIO.getRecordReader();

        recordReader.read();
        recordReader.read();

        read(recordReader, 10000);
        read(recordReader, 10000);
        read(recordReader, 10000);
        read(recordReader, 100000);
        read(recordReader, 1000000);
      }
    }
    {
      MessageColumnIO columnIO = new ColumnIOFactory(new SimpleGroupFactory(TestColumnIO.schema2)).getColumnIO(TestColumnIO.schema2, columns);

      System.out.println("read projected");
      {
        RecordReader recordReader = columnIO.getRecordReader();

        recordReader.read();
        recordReader.read();

        read(recordReader, 10000);
        read(recordReader, 10000);
        read(recordReader, 10000);
        read(recordReader, 100000);
        read(recordReader, 1000000);
      }
    }
    System.out.println(columns.memSize()+" bytes used total");
    System.out.println("max col size: "+columns.maxColMemSize()+" bytes");
  }

  private static void read(RecordReader recordReader, int count) {
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      recordReader.read();
    }
    long t1 = System.currentTimeMillis();
    System.out.println("read "+count+ " in " +(float)(t1-t0)/count+" ms/rec");
  }

  private static void write(RecordWriter recordWriter, int count) {
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      recordWriter.write(TestColumnIO.r1);
    }
    long t1 = System.currentTimeMillis();
    System.out.println("written "+count+ " in " +(float)(t1-t0)/count+" ms/rec");
  }

}
