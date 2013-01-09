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

import java.util.logging.Level;

import redelm.Log;
import redelm.column.mem.MemColumnsStore;
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
    MemColumnsStore columns = new MemColumnsStore(50*1024*1024, schema);
    write(columns);
//    columns.flip();
    read(columns);
    System.out.println(columns.memSize()+" bytes used total");
    System.out.println("max col size: "+columns.maxColMemSize()+" bytes");
  }

  private static void read(MemColumnsStore columns) {
      read(columns, schema, "read all", false);
      read(columns, schema, "read all", false);
      read(columns, schema, "read all", true);
      read(columns, schema, "read all", true);
      read(columns, schema2, "read projected", false);
      read(columns, schema2, "read projected", true);
      read(columns, schema3, "read projected no Strings", false);
      read(columns, schema3, "read projected no Strings", true);
  }

  private static void read(MemColumnsStore columns, MessageType myschema,
      String message, boolean compiled) {
    columns.flip();
    MessageColumnIO columnIO = newColumnFactory(myschema);
    System.out.println(message + (compiled ? " compiled" : ""));
    RecordMaterializer<Object> recordConsumer = new RecordMaterializer<Object>() {
      Object a;
      public void startMessage() { a = "startmessage";}
      public void startGroup() { a = "startgroup";}
      public void startField(String field, int index) { a = field;}
      public void endMessage() { a = "endmessage";}
      public void endGroup() { a = "endgroup";}
      public void endField(String field, int index) { a = field;}
      public void addString(String value) { a = value;}
      public void addInteger(int value) { a = "int";}
      public void addLong(long value) { a = "long";}
      public void addFloat(float value) { a = "float";}
      public void addDouble(double value) { a = "double";}
      public void addBoolean(boolean value) { a = "boolean";}
      public void addBinary(byte[] value) { a = value;}
      public Object getCurrentRecord() { return a; }
    };
    RecordReader<Object> recordReader = columnIO.getRecordReader(columns, recordConsumer);
    if (compiled) {
      recordReader = new RecordReaderCompiler().compile((RecordReaderImplementation<Object>)recordReader);
    }
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
    MessageColumnIO columnIO = newColumnFactory(schema);

    GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), schema);
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

  private static MessageColumnIO newColumnFactory(MessageType schema) {
    return new ColumnIOFactory().getColumnIO(schema);
  }

  private static void read(RecordReader<Object> recordReader, int count, MessageType schema) {
    Object[] records = new Object[count];
    System.gc();
    System.out.println("<<<");
    long t0 = System.currentTimeMillis();
    recordReader.read(records, count);
    long t1 = System.currentTimeMillis();
    System.out.println(">>>");
    long t = t1-t0;
    float err = (float)100 * 2 / t; // (+/- 1 ms)
    System.out.printf("read %,9d recs in %,5d ms at %,9d rec/s err: %3.2f%%\n", count , t, t == 0 ? 0 : count * 1000 / t, err);
    if (!records[0].equals("endmessage")) {
      throw new RuntimeException();
    }
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
