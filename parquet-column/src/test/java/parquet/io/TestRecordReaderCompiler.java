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
package parquet.io;

import static parquet.example.Paper.r1;
import static parquet.example.Paper.schema;
import static parquet.io.TestColumnIO.expectedEventsForR1;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


import org.junit.Test;

import parquet.column.ColumnWriteStore;
import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.column.page.mem.MemPageStore;
import parquet.example.data.GroupWriter;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.io.RecordReaderCompiler;
import parquet.io.RecordReaderImplementation;

public class TestRecordReaderCompiler {

  @Test
  public void testRecordReaderCompiler() {

    Logger.getLogger("brennus").setLevel(Level.FINEST);
    Logger.getLogger("brennus").addHandler(new Handler() {
      public void publish(LogRecord record) {
        System.err.println(record.getMessage());
      }
      public void flush() {
        System.err.flush();
      }
      public void close() throws SecurityException {
        System.err.flush();
      }
    });

    MemPageStore memPageStore = new MemPageStore();
    ColumnWriteStore writeStore = new ColumnWriteStoreImpl(memPageStore, 1024*1024*1, 1024*1024*1, false);
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    new GroupWriter(columnIO.getRecordWriter(writeStore), schema).write(r1);
    writeStore.flush();
    System.err.flush();
    Logger.getLogger("brennus").info("compile");
    System.out.println("compile");
    RecordReader<Void> recordReader = columnIO.getRecordReader(
        memPageStore,
        new ExpectationValidatingConverter(
            new ArrayDeque<String>(Arrays.asList(expectedEventsForR1)), schema));
    recordReader = new RecordReaderCompiler().compile((RecordReaderImplementation<Void>)recordReader);

    Logger.getLogger("brennus").info("read");
    System.out.println("read");
    recordReader.read();

  }
}
