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

import static parquet.pig.GenerateIntTestFile.readTestFile;
import static parquet.pig.GenerateIntTestFile.writeToFile;

import java.io.File;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.Log;
import parquet.column.mem.MemColumnWriteStore;
import parquet.column.mem.MemPageStore;
import parquet.io.Binary;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

public class GenerateTPCH {
  private static final Log LOG = Log.getLog(GenerateTPCH.class);

  public static void main(String[] args) throws IOException {
    File out = new File("testdata/from_java/tpch/customer");
    if (out.exists()) {
      if (!out.delete()) {
        throw new RuntimeException("can not remove existing file " + out.getAbsolutePath());
      }
    }
    Path testFile = new Path(out.toURI());
    Configuration configuration = new Configuration();
    MessageType schema = new MessageType("customer",
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32,  "c_custkey"),
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_name"),
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_address"),
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32,  "c_nationkey"),
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_phone"),
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "c_acctbal"),
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_mktsegment"),
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_comment")
        );

    MemPageStore pageStore = new MemPageStore();
    MemColumnWriteStore store = new MemColumnWriteStore(pageStore, 8*1024);
    //
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

    RecordConsumer recordWriter = columnIO.getRecordWriter(store);

    int recordCount = 0;
    for (int i = 0; i < 150000; i++) {
      recordWriter.startMessage();
      writeField(recordWriter, 0, "c_custkey", i % 10 == 0 ? null : i);
      writeField(recordWriter, 1, "c_name", i % 11 == 0 ? null : "name_" + i);
      writeField(recordWriter, 2, "c_address", i % 12 == 0 ? null : "add_" + i);
      writeField(recordWriter, 3, "c_nationkey", i % 13 == 0 ? null : i);
      writeField(recordWriter, 4, "c_phone", i % 14 == 0 ? null : "phone_" + i);
      writeField(recordWriter, 5, "c_acctbal", i % 15 == 0 ? null : 1.2d * i);
      writeField(recordWriter, 6, "c_mktsegment", i % 16 == 0 ? null : "mktsegment_" + i);
      writeField(recordWriter, 7, "c_comment", i % 17 == 0 ? null : "comment_" + i);
      recordWriter.endMessage();
      ++ recordCount;
    }
    store.flush();

    writeToFile(testFile, configuration, schema, pageStore, recordCount);

    try {
      readTestFile(testFile, configuration);
    } catch (Exception e) {
      LOG.error("failed reading", e);
    }

  }

  private static void writeField(RecordConsumer recordWriter, int index, String name, Object value) {
    recordWriter.startField(name, index);
    if (value != null) {
      if (value instanceof Integer) {
        recordWriter.addInteger((Integer)value);
      } else if (value instanceof String) {
        recordWriter.addBinary(Binary.fromString((String)value));
      } else if (value instanceof Double) {
        recordWriter.addDouble((Double)value);
      } else {
        throw new IllegalArgumentException(value.getClass().getName() + " not supported");
      }
    }
    recordWriter.endField(name, index);
  }
}
