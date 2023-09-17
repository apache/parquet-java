/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.pig;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class TupleConsumerPerfTest {

  private static final int TOP_LEVEL_COLS = 1;

  public static void main(String[] args) throws Exception {
    String pigSchema = pigSchema(false, false);
    String pigSchemaProjected = pigSchema(true, false);
    String pigSchemaNoString = pigSchema(true, true);
    MessageType schema = new PigSchemaConverter().convert(Utils.getSchemaFromString(pigSchema));

    MemPageStore memPageStore = new MemPageStore(0);
    ColumnWriteStoreV1 columns = new ColumnWriteStoreV1(
        memPageStore, ParquetProperties.builder()
            .withPageSize(50*1024*1024)
            .withDictionaryEncoding(false)
            .build());
    write(memPageStore, columns, schema, pigSchema);
    columns.flush();
    read(memPageStore, pigSchema, pigSchemaProjected, pigSchemaNoString);
    System.out.println(columns.getBufferedSize()+" bytes used total");
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
    Map<String, String> pigMetaData = pigMetaData(pigSchemaString);
    MessageType schema = new PigSchemaConverter().convert(Utils.getSchemaFromString(pigSchemaString));
    ReadContext init = tupleReadSupport.init((ParquetConfiguration) null, pigMetaData, schema);
    RecordMaterializer<Tuple> recordConsumer = tupleReadSupport.prepareForRead((ParquetConfiguration) null, pigMetaData, schema, init);
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

  private static void write(MemPageStore memPageStore, ColumnWriteStoreV1 columns, MessageType schema, String pigSchemaString) throws ExecException, ParserException {
    MessageColumnIO columnIO = newColumnFactory(pigSchemaString);
    TupleWriteSupport tupleWriter = TupleWriteSupport.fromPigSchema(pigSchemaString);
    tupleWriter.init((ParquetConfiguration) null);
    tupleWriter.prepareForWrite(columnIO.getRecordWriter(columns));
    write(memPageStore, tupleWriter, 10000);
    write(memPageStore, tupleWriter, 10000);
    write(memPageStore, tupleWriter, 10000);
    write(memPageStore, tupleWriter, 10000);
    write(memPageStore, tupleWriter, 10000);
    write(memPageStore, tupleWriter, 100000);
    write(memPageStore, tupleWriter, 1000000);
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

  private static void write(MemPageStore memPageStore, TupleWriteSupport tupleWriter, int count) throws ExecException {
    Tuple tu = tuple();
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      tupleWriter.write(tu);
    }
    long t1 = System.currentTimeMillis();
    long t = t1-t0;
    memPageStore.addRowCount(count);
    System.out.printf("written %,9d recs in %,5d ms at %,9d rec/s\n", count, t, t == 0 ? 0 : count * 1000 / t );
  }


}
