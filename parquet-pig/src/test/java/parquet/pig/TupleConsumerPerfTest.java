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
package parquet.pig;

import static parquet.Log.DEBUG;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;


import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.Log;
import parquet.column.mem.MemColumnWriteStore;
import parquet.column.mem.MemPageStore;
import parquet.column.mem.PageReadStore;
import parquet.example.DummyRecordConverter;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordConsumer;
import parquet.io.RecordConsumerLoggingWrapper;
import parquet.io.RecordMaterializer;
import parquet.io.RecordReader;
import parquet.io.convert.RecordConverter;
import parquet.pig.PigMetaData;
import parquet.pig.PigSchemaConverter;
import parquet.pig.TupleReadSupport;
import parquet.pig.TupleWriteSupport;
import parquet.schema.MessageType;

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

    MemPageStore memPageStore = new MemPageStore();
    MemColumnWriteStore columns = new MemColumnWriteStore(memPageStore, 50*1024*1024);
    write(columns, schema, pigSchema);
    columns.flush();
    read(memPageStore, pigSchema, pigSchemaProjected, pigSchemaNoString);
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

  private static void read(PageReadStore columns, String pigSchemaString, String pigSchemaProjected, String pigSchemaProjectedNoStrings) throws ParserException {
      read(columns, pigSchemaString, "read all");
      read(columns, pigSchemaProjected, "read projected");
      read(columns, pigSchemaProjectedNoStrings, "read projected no Strings");
  }

  private static void read(PageReadStore columns, String pigSchemaString, String message) throws ParserException {
    System.out.println(message);
    MessageColumnIO columnIO = newColumnFactory(pigSchemaString);
    TupleReadSupport tupleReadSupport = new TupleReadSupport();
    MessageType schema = new PigSchemaConverter().convert(Utils.getSchemaFromString(pigSchemaString));
    RecordConverter<Tuple> recordConsumer = tupleReadSupport.initForRead(null, pigMetaData(pigSchemaString), schema, schema);
    RecordReader<Tuple> recordReader = columnIO.getRecordReader(columns, recordConsumer);
    // TODO: put this back
//  if (DEBUG) {
//    recordConsumer = new RecordConsumerLoggingWrapper(recordConsumer);
//  }
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 10000, pigSchemaString);
    read(recordReader, 100000, pigSchemaString);
    read(recordReader, 1000000, pigSchemaString);
    System.out.println();
  }

  private static Map<String, String> pigMetaData(String pigSchemaString) {
    Map<String, String> map = new HashMap<String, String>();
    new PigMetaData(pigSchemaString).addToMetaData(map);
    return map;
  }

  private static void write(MemColumnWriteStore columns, MessageType schema, String pigSchemaString) throws ExecException, ParserException {
    MessageColumnIO columnIO = newColumnFactory(pigSchemaString);
    TupleWriteSupport tupleWriter = new TupleWriteSupport(schema, Utils.getSchemaFromString(pigSchemaString));
    tupleWriter.init(null);
    tupleWriter.prepareForWrite(columnIO.getRecordWriter(columns));
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


  private static void read(RecordReader<Tuple> recordReader, int count, String pigSchemaString) throws ParserException {

    long t0 = System.currentTimeMillis();
    Tuple tuple = null;
    for (int i = 0; i < count; i++) {
      tuple = recordReader.read();
    }
    if (tuple == null) {
      throw new RuntimeException();
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
