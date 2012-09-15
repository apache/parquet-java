package redelm.io;

import static redelm.data.simple.example.Paper.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;

import redelm.Log;
import redelm.column.mem.MemColumnsStore;
import redelm.data.Group;
import redelm.data.GroupRecordConsumer;
import redelm.data.GroupWriter;
import redelm.data.simple.SimpleGroupFactory;
import redelm.schema.MessageType;

/**
 * make sure {@link Log#LEVEL} is set to {@link Level#OFF}
 *
 * @author Julien Le Dem
 *
 */
public class PerfTest {

  public static void main(String[] args) {
    MemColumnsStore columns = new MemColumnsStore(50*1024*1024);
    {
      MessageColumnIO columnIO = newColumnFactory(columns, schema);

      {
        GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(), schema);
        groupWriter.write(r1);
        groupWriter.write(r2);

        write(groupWriter, 10000);
        write(groupWriter, 10000);
        write(groupWriter, 10000);
        write(groupWriter, 100000);
        write(groupWriter, 1000000);
      }
      System.out.println("read all");
      {
        RecordReader recordReader = columnIO.getRecordReader();

        read(recordReader, 2, schema);

        read(recordReader, 10000, schema);
        read(recordReader, 10000, schema);
        read(recordReader, 10000, schema);
        read(recordReader, 100000, schema);
        read(recordReader, 1000000, schema);
      }
    }
    {
      MessageColumnIO columnIO = newColumnFactory(columns, schema2);

      System.out.println("read projected");
      {
        RecordReader recordReader = columnIO.getRecordReader();

        read(recordReader, 2, schema2);

        read(recordReader, 10000, schema2);
        read(recordReader, 10000, schema2);
        read(recordReader, 10000, schema2);
        read(recordReader, 100000, schema2);
        read(recordReader, 1000000, schema2);
      }
    }
    {
      MessageColumnIO columnIO = newColumnFactory(columns, schema3);

      System.out.println("read projected no Strings");
      {
        RecordReader recordReader = columnIO.getRecordReader();

        read(recordReader, 2, schema3);

        read(recordReader, 10000, schema3);
        read(recordReader, 10000, schema3);
        read(recordReader, 10000, schema3);
        read(recordReader, 100000, schema3);
        read(recordReader, 1000000, schema3);
      }
    }
    System.out.println(columns.memSize()+" bytes used total");
    System.out.println("max col size: "+columns.maxColMemSize()+" bytes");
  }

  private static MessageColumnIO newColumnFactory(MemColumnsStore columns,
      MessageType schema) {
    return new ColumnIOFactory().getColumnIO(schema, columns);
  }

  private static void read(RecordReader recordReader, int count, MessageType schema) {
    Collection<Group> result = new Collection<Group>() {
      public int size() {
        return 0;
      }
      public boolean isEmpty() {
        return false;
      }
      public boolean contains(Object o) {
        return false;
      }
      public Iterator<Group> iterator() {
        return null;
      }
      public Object[] toArray() {
        return null;
      }
      public <T> T[] toArray(T[] a) {
        return null;
      }
      public boolean add(Group e) {
        return false;
      }
      public boolean remove(Object o) {
        return false;
      }
      public boolean containsAll(Collection<?> c) {
        return false;
      }
      public boolean addAll(Collection<? extends Group> c) {
        return false;
      }
      public boolean removeAll(Collection<?> c) {
        return false;
      }
      public boolean retainAll(Collection<?> c) {
        return false;
      }
      public void clear() {
      }
    };
    RecordConsumer recordConsumer = new GroupRecordConsumer(new SimpleGroupFactory(schema), result);
    if (Log.DEBUG) {
      recordConsumer = new RecordConsumerWrapper(recordConsumer);
    }
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      recordReader.read(recordConsumer);
    }
    long t1 = System.currentTimeMillis();
    System.out.println("read "+count+ " in " +(float)(t1-t0)*1000/count+" µs/rec");
  }

  private static void write(GroupWriter groupWriter, int count) {
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      groupWriter.write(r1);
    }
    long t1 = System.currentTimeMillis();
    System.out.println("written "+count+ " in " +(float)(t1-t0)*1000/count+" µs/rec");
  }

}
