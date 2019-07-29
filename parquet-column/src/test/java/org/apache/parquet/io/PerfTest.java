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
package org.apache.parquet.io;

import static org.apache.parquet.example.Paper.r1;
import static org.apache.parquet.example.Paper.r2;
import static org.apache.parquet.example.Paper.schema;
import static org.apache.parquet.example.Paper.schema2;
import static org.apache.parquet.example.Paper.schema3;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;


public class PerfTest {

  public static void main(String[] args) {
    MemPageStore memPageStore = new MemPageStore(0);
    write(memPageStore);
    read(memPageStore);
  }

  private static void read(MemPageStore memPageStore) {
    read(memPageStore, schema, "read all");
    read(memPageStore, schema, "read all");
    read(memPageStore, schema2, "read projected");
    read(memPageStore, schema3, "read projected no Strings");
  }

  private static void read(MemPageStore memPageStore, MessageType myschema,
      String message) {
    MessageColumnIO columnIO = newColumnFactory(myschema);
    System.out.println(message);
    RecordMaterializer<Object> recordConsumer = new DummyRecordConverter(myschema);
    RecordReader<Object> recordReader = columnIO.getRecordReader(memPageStore, recordConsumer);

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


  private static void write(MemPageStore memPageStore) {
    ColumnWriteStoreV1 columns = new ColumnWriteStoreV1(
        memPageStore,
        ParquetProperties.builder()
            .withPageSize(50*1024*1024)
            .withDictionaryEncoding(false)
            .build());
    MessageColumnIO columnIO = newColumnFactory(schema);

    GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), schema);
    groupWriter.write(r1);
    groupWriter.write(r2);

    write(memPageStore, groupWriter, 10000);
    write(memPageStore, groupWriter, 10000);
    write(memPageStore, groupWriter, 10000);
    write(memPageStore, groupWriter, 10000);
    write(memPageStore, groupWriter, 10000);
    write(memPageStore, groupWriter, 100000);
    write(memPageStore, groupWriter, 1000000);
    columns.flush();
    System.out.println();
    System.out.println(columns.getBufferedSize() + " bytes used total");
    System.out.println("max col size: "+columns.maxColMemSize()+" bytes");
  }

  private static MessageColumnIO newColumnFactory(MessageType schema) {
    return new ColumnIOFactory().getColumnIO(schema);
  }
  private static void read(RecordReader<Object> recordReader, int count, MessageType schema) {
    Object[] records = new Object[count];
    System.gc();
    System.out.print("no gc <");
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < records.length; i++) {
      records[i] = recordReader.read();
    }
    long t1 = System.currentTimeMillis();
    System.out.print("> ");
    long t = t1-t0;
    float err = (float)100 * 2 / t; // (+/- 1 ms)
    System.out.printf("                                            read %,9d recs in %,5d ms at %,9d rec/s err: %3.2f%%\n", count , t, t == 0 ? 0 : count * 1000 / t, err);
    if (!records[0].equals("end()")) {
      throw new RuntimeException(""+records[0]);
    }
  }

  private static void write(MemPageStore memPageStore, GroupWriter groupWriter, int count) {
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      groupWriter.write(r1);
    }
    long t1 = System.currentTimeMillis();
    long t = t1-t0;
    memPageStore.addRowCount(count);
    System.out.printf("written %,9d recs in %,5d ms at %,9d rec/s\n", count, t, t == 0 ? 0 : count * 1000 / t );
  }

}
