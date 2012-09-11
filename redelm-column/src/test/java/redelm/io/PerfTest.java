package redelm.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import redelm.Log;
import redelm.column.mem.MemColumnsStore;
import redelm.data.Group;
import redelm.data.simple.SimpleGroupFactory;
import redelm.data.simple.SimpleGroupRecordConsumer;
import redelm.schema.MessageType;


public class PerfTest {

  public static void main(String[] args) {
    MemColumnsStore columns = new MemColumnsStore(50*1024*1024);
    {
      MessageType schema = TestColumnIO.schema;
      MessageColumnIO columnIO = newColumnFactory(columns, schema);

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

        read(recordReader, 2, schema);

        read(recordReader, 10000, schema);
        read(recordReader, 10000, schema);
        read(recordReader, 10000, schema);
        read(recordReader, 100000, schema);
        read(recordReader, 1000000, schema);
      }
    }
    {
      MessageType schema2 = TestColumnIO.schema2;
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
      MessageType schema3 = TestColumnIO.schema3;
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
    return new ColumnIOFactory(new SimpleGroupFactory(schema)).getColumnIO(schema, columns);
  }

  private static void read(RecordReader recordReader, int count, MessageType schema) {
    List<Group> result = new List<Group>() {

      @Override
      public int size() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public Iterator<Group> iterator() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Object[] toArray() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public <T> T[] toArray(T[] a) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public boolean add(Group e) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean remove(Object o) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean addAll(Collection<? extends Group> c) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean addAll(int index, Collection<? extends Group> c) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public void clear() {
        // TODO Auto-generated method stub

      }

      @Override
      public Group get(int index) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Group set(int index, Group element) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void add(int index, Group element) {
        // TODO Auto-generated method stub

      }

      @Override
      public Group remove(int index) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public int indexOf(Object o) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public int lastIndexOf(Object o) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public ListIterator<Group> listIterator() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public ListIterator<Group> listIterator(int index) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public List<Group> subList(int fromIndex, int toIndex) {
        // TODO Auto-generated method stub
        return null;
      }
    };
    RecordConsumer recordConsumer = new SimpleGroupRecordConsumer(new SimpleGroupFactory(schema), result);
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

  private static void write(RecordWriter recordWriter, int count) {
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      recordWriter.write(TestColumnIO.r1);
    }
    long t1 = System.currentTimeMillis();
    System.out.println("written "+count+ " in " +(float)(t1-t0)*1000/count+" µs/rec");
  }

}
