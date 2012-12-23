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
package redelm.pig;

import static redelm.Log.DEBUG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;

import redelm.Log;
import redelm.column.mem.MemColumnsStore;
import redelm.io.ColumnIOFactory;
import redelm.io.MessageColumnIO;
import redelm.io.RecordConsumer;
import redelm.io.RecordConsumerWrapper;
import redelm.io.RecordReader;
import redelm.schema.MessageType;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * make sure {@link Log#LEVEL} is set to {@link Level#OFF}
 *
 * @author Julien Le Dem
 *
 */
public class TupleConsumerPerfTest {

  private static final int TOP_LEVEL_COLS = 1;

  public static void main(String[] args) throws Exception {
    String pigSchema = pigSchema(false, false);
    String pigSchemaProjected = pigSchema(true, false);
    String pigSchemaNoString = pigSchema(true, true);
    PigSchemaConverter pigSchemaConverter = new PigSchemaConverter();
    MessageType schema = pigSchemaConverter.convert(Utils.getSchemaFromString(pigSchema));

    MemColumnsStore columns = new MemColumnsStore(50*1024*1024, schema);
    write(columns, schema, pigSchema);
    columns.flip();
    read(columns, pigSchema, pigSchemaProjected, pigSchemaNoString);
    System.out.println(columns.memSize()+" bytes used total");
    System.out.println("max col size: "+columns.maxColMemSize()+" bytes");
  }

  private static String pigSchema(boolean projected, boolean noStrings) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < TOP_LEVEL_COLS; i++) {
      if (i!=0) {
        sb.append(", ");
      }
      sb.append("i"+i+":(");
      if (!noStrings) {
        for (int j = 0; j < (projected ? 2 : 4); j++)  {
          if (j!=0) {
            sb.append(", ");
          }
          sb.append("j"+j+":chararray");
        }
        sb.append(", ");
      }
      for (int k = 0; k < (projected ? 2 : 4); k++)  {
        if (k!=0) {
          sb.append(", ");
        }
        sb.append("k"+k+":long");
      }
      for (int l = 0; l < (projected ? 1 : 2); l++)  {
        sb.append(", ");
        sb.append("l"+l+":{t:(v:int)}");
      }
      sb.append(")");
    }
    return sb.toString();
  }

  private static Tuple tuple() throws ExecException {
    TupleFactory tf = TupleFactory.getInstance();
    Tuple t = tf.newTuple(TOP_LEVEL_COLS);
    for (int i = 0; i < TOP_LEVEL_COLS; i++) {
      Tuple ti = tf.newTuple(10);
      for (int j = 0; j < 4; j++)  {
        ti.set(j, "foo"+i+","+j);
      }
      for (int k = 0; k < 4; k++)  {
        ti.set(4+k, (long)k);
      }
      for (int l = 0; l < 2; l++)  {
        DataBag bag = new NonSpillableDataBag();
        for (int m = 0; m < 10; m++) {
          bag.add(tf.newTuple((Object)new Integer(m)));
        }
        ti.set(8+l, bag);
      }
      t.set(i, ti);
    }
    return t;
  }

  private static void read(MemColumnsStore columns, String pigSchemaString, String pigSchemaProjected, String pigSchemaProjectedNoStrings) throws ParserException {
      read(columns, pigSchemaString, "read all");
      read(columns, pigSchemaProjected, "read projected");
      read(columns, pigSchemaProjectedNoStrings, "read projected no Strings");
  }

  private static void read(MemColumnsStore columns,
      String pigSchemaString, String message) throws ParserException {
    MessageColumnIO columnIO = newColumnFactory(pigSchemaString);
    System.out.println(message);
    RecordReader recordReader = columnIO.getRecordReader(columns, null);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 100000, pigSchemaString);
    read(recordReader, 1000000, pigSchemaString);
    System.out.println();
  }

  private static void write(MemColumnsStore columns, MessageType schema, String pigSchemaString) throws ExecException, ParserException {
    MessageColumnIO columnIO = newColumnFactory(pigSchemaString);
    TupleWriteSupport tupleWriter = new TupleWriteSupport();
    tupleWriter.initForWrite(columnIO.getRecordWriter(columns), schema, Arrays.asList(new PigMetaData(pigSchemaString).toMetaDataBlock()));
    write(tupleWriter, 10000);
    write(tupleWriter, 10000);
    write(tupleWriter, 10000);
    write(tupleWriter, 10000);
    write(tupleWriter, 10000);
    write(tupleWriter, 100000);
    write(tupleWriter, 1000000);
    System.out.println();
  }

  private static MessageColumnIO newColumnFactory(String pigSchemaString) throws ParserException {
    MessageType schema = new PigSchemaConverter().convert(Utils.getSchemaFromString(pigSchemaString));
    return new ColumnIOFactory().getColumnIO(schema);
  }

  private static void read(RecordReader recordReader, int count, String pigSchemaString) throws ParserException {
    List<Tuple> result = new List<Tuple>() {
      Tuple current;
      public int size() {
        return 0;
      }
      public boolean isEmpty() {
        return false;
      }
      public boolean contains(Object o) {
        return false;
      }
      public Iterator<Tuple> iterator() {
        return null;
      }
      public Object[] toArray() {
        return null;
      }
      public <T> T[] toArray(T[] a) {
        return null;
      }
      public boolean add(Tuple e) {
        current = e;
        return false;
      }
      public boolean remove(Object o) {
        return false;
      }
      public boolean containsAll(Collection<?> c) {
        return false;
      }
      public boolean addAll(Collection<? extends Tuple> c) {
        return false;
      }
      public boolean removeAll(Collection<?> c) {
        return false;
      }
      public boolean retainAll(Collection<?> c) {
        return false;
      }
      public void clear() {}
      public boolean addAll(int index, Collection<? extends Tuple> c) {
        return false;
      }
      public Tuple get(int index) {
        return null;
      }
      public Tuple set(int index, Tuple element) {
        return null;
      }
      public void add(int index, Tuple element) {
      }
      public Tuple remove(int index) {
        return null;
      }
      public int indexOf(Object o) {
        return 0;
      }
      public int lastIndexOf(Object o) {
        return 0;
      }
      public ListIterator<Tuple> listIterator() {
        return null;
      }
      public ListIterator<Tuple> listIterator(int index) {
        return null;
      }
      public List<Tuple> subList(int fromIndex, int toIndex) {
        return null;
      }
    };
    TupleReadSupport tupleReadSupport = new TupleReadSupport();
    MessageType schema = new PigSchemaConverter().convert(Utils.getSchemaFromString(pigSchemaString));
    tupleReadSupport.initForRead(Arrays.asList(new PigMetaData(pigSchemaString).toMetaDataBlock()), schema.toString());
    RecordConsumer recordConsumer = tupleReadSupport.newRecordConsumer();
    if (DEBUG) {
      recordConsumer = new RecordConsumerWrapper(recordConsumer);
    }
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      recordReader.read();
    }
    long t1 = System.currentTimeMillis();
    long t = t1-t0;
    float err = (float)100 * 2 / t; // (+/- 1 ms)
    System.out.printf("read %,9d recs in %,5d ms at %,9d rec/s err: %3.2f%%\n", count , t, t == 0 ? 0 : count * 1000 / t, err);
  }

  private static void write(TupleWriteSupport tupleWriter, int count) throws ExecException {
    Tuple tu = tuple();
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      tupleWriter.write(tu);
    }
    long t1 = System.currentTimeMillis();
    long t = t1-t0;
    System.out.printf("written %,9d recs in %,5d ms at %,9d rec/s\n", count, t, t == 0 ? 0 : count * 1000 / t );
  }


}
