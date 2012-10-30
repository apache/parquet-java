/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redelm.io;

import static redelm.data.simple.example.Paper.r1;
import static redelm.data.simple.example.Paper.r2;
import static redelm.data.simple.example.Paper.schema;
import static redelm.data.simple.example.Paper.schema2;
import static redelm.data.simple.example.Paper.schema3;

import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;

import redelm.Log;
import redelm.column.mem.MemColumnsStore;
import redelm.data.Group;
import redelm.data.GroupWriter;
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
    write(columns);
    read(columns);
    System.out.println(columns.memSize()+" bytes used total");
    System.out.println("max col size: "+columns.maxColMemSize()+" bytes");
  }

  private static void read(MemColumnsStore columns) {
      read(columns, schema, "read all");
      read(columns, schema2, "read projected");
      read(columns, schema3, "read projected no Strings");
  }

  private static void read(MemColumnsStore columns, MessageType myschema,
      String message) {
    MessageColumnIO columnIO = newColumnFactory(columns, myschema);
    System.out.println(message);
    RecordReader recordReader = columnIO.getRecordReader();
    read(recordReader, 2, myschema);
    read(recordReader, 10000, myschema);
    read(recordReader, 10000, myschema);
    read(recordReader, 10000, myschema);
    read(recordReader, 10000, myschema);
    read(recordReader, 10000, myschema);
    read(recordReader, 100000, myschema);
    read(recordReader, 1000000, myschema);
    System.out.println();
  }

  private static void write(MemColumnsStore columns) {
    MessageColumnIO columnIO = newColumnFactory(columns, schema);

    GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(), schema);
    groupWriter.write(r1);
    groupWriter.write(r2);

    write(groupWriter, 10000);
    write(groupWriter, 10000);
    write(groupWriter, 10000);
    write(groupWriter, 10000);
    write(groupWriter, 10000);
    write(groupWriter, 100000);
    write(groupWriter, 1000000);
    System.out.println();
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
//    RecordConsumer recordConsumer = new GroupRecordConsumer(new SimpleGroupFactory(schema), result);
    RecordConsumer recordConsumer = new RecordConsumer() {
      public void startMessage() {}
      public void startGroup() {}
      public void startField(String field, int index) {}
      public void endMessage() {}
      public void endGroup() {}
      public void endField(String field, int index) {}
      public void addString(String value) {}
      public void addInteger(int value) {}
      public void addLong(long value) {}
      public void addFloat(float value) {}
      public void addDouble(double value) {}
      public void addBoolean(boolean value) {}
      public void addBinary(byte[] value) {}
    };
    if (Log.DEBUG) {
      recordConsumer = new RecordConsumerWrapper(recordConsumer);
    }
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      recordReader.read(recordConsumer);
    }
    long t1 = System.currentTimeMillis();
    long t = t1-t0;
    System.out.printf("read %,9d recs in %,5d ms at %,9d rec/s\n", count , t, t == 0 ? 0 : count * 1000 / t);
  }

  private static void write(GroupWriter groupWriter, int count) {
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      groupWriter.write(r1);
    }
    long t1 = System.currentTimeMillis();
    long t = t1-t0;
    System.out.printf("written %,9d recs in %,5d ms at %,9d rec/s\n", count, t, t == 0 ? 0 : count * 1000 / t );
  }

}
