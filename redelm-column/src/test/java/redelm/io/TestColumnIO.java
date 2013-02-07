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

import static org.junit.Assert.assertEquals;
import static redelm.data.simple.example.Paper.pr1;
import static redelm.data.simple.example.Paper.pr2;
import static redelm.data.simple.example.Paper.r1;
import static redelm.data.simple.example.Paper.r2;
import static redelm.data.simple.example.Paper.schema;
import static redelm.data.simple.example.Paper.schema2;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import org.junit.Test;

import redelm.Log;
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.column.ColumnsStore;
import redelm.column.mem.MemColumnsStore;
import redelm.column.mem.MemPageStore;
import redelm.data.Group;
import redelm.data.GroupRecordConsumer;
import redelm.data.GroupWriter;
import redelm.data.simple.SimpleGroupFactory;
import redelm.schema.MessageType;

public class TestColumnIO {
  private static final Log LOG = Log.getLog(TestColumnIO.class);

  private static final String schemaString =
      "message Document {\n"
    + "  required int64 DocId;\n"
    + "  optional group Links {\n"
    + "    repeated int64 Backward;\n"
    + "    repeated int64 Forward;\n"
    + "  }\n"
    + "  repeated group Name {\n"
    + "    repeated group Language {\n"
    + "      required binary Code;\n"
    + "      optional binary Country;\n"
    + "    }\n"
    + "    optional binary Url;\n"
    + "  }\n"
    + "}\n";

  int[][] expectedFSA = new int[][] {
      { 1 },      // 0: DocId
      { 2, 1 },   // 1: Links.Backward
      { 3, 2 },   // 2: Links.Forward
      { 4, 4, 4 },// 3: Name.Language.Code
      { 5, 5, 3 },// 4: Name.Language.Country
      { 6, 3 }    // 5: Name.Url
  };

  int[][] expectedFSA2 = new int[][] {
      { 1 },      // 0: DocId
      { 2, 1, 1 },// 1: Name.Language.Country
  };

  @Test
  public void testSchema() {
    assertEquals(schemaString, schema.toString());
  }

  @Test
  public void testColumnIO() {
    log(schema);
    log("r1");
    log(r1);
    log("r2");
    log(r2);

    ColumnsStore columns = new MemColumnsStore(1024, new MemPageStore(), 800);

    ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
    {
      MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
      log(columnIO);
      GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), schema);
      groupWriter.write(r1);
      groupWriter.write(r2);
      columns.flush();
      log(columns);
      log("=========");
      RecordReader<Group> recordReader = getRecordReader(columnIO, schema, columns);

      validateFSA(expectedFSA, columnIO, recordReader);

      List<Group> records = new ArrayList<Group>();
      read(recordReader, schema, records);
      read(recordReader, schema, records);

      int i = 0;
      for (Group record : records) {
        log("r" + (++i));
        log(record);
      }

      assertEquals("deserialization does not display the same result", r1.toString(), records.get(0).toString());
      assertEquals("deserialization does not display the same result", r2.toString(), records.get(1).toString());
    }
    {
      MessageColumnIO columnIO2 = columnIOFactory.getColumnIO(schema2);


      List<Group> records = new ArrayList<Group>();
      RecordReader<Group> recordReader = getRecordReader(columnIO2, schema2, columns);

      validateFSA(expectedFSA2, columnIO2, recordReader);

      read(recordReader, schema2, records);
      read(recordReader, schema2, records);

      int i = 0;
      for (Group record : records) {
        log("r" + (++i));
        log(record);
      }
      assertEquals("deserialization does not display the expected result", pr1.toString(), records.get(0).toString());
      assertEquals("deserialization does not display the expected result", pr2.toString(), records.get(1).toString());
    }
  }

  private void log(Object o) {
    LOG.info(o);
  }

  private RecordReader<Group> getRecordReader(MessageColumnIO columnIO, MessageType schema, ColumnsStore columns) {
    RecordMaterializer<Group> recordConsumer = new GroupRecordConsumer(new SimpleGroupFactory(schema));
    return columnIO.getRecordReader(columns, recordConsumer);
  }

  private void validateFSA(int[][] expectedFSA, MessageColumnIO columnIO, RecordReader<?> recordReader) {
    log("FSA: ----");
    List<PrimitiveColumnIO> leaves = columnIO.getLeaves();
    for (int i = 0; i < leaves.size(); ++i) {
      PrimitiveColumnIO primitiveColumnIO = leaves.get(i);
      log(Arrays.toString(primitiveColumnIO.getFieldPath()));
      for (int r = 0; r < expectedFSA[i].length; r++) {
        int next = expectedFSA[i][r];
        log(" "+r+" -> "+ (next==leaves.size() ? "end" : Arrays.toString(leaves.get(next).getFieldPath()))+": "+recordReader.getNextLevel(i, r));
        assertEquals(Arrays.toString(primitiveColumnIO.getFieldPath())+": "+r+" -> ", next, recordReader.getNextReader(i, r));
      }
    }
    log("----");
  }

  @Test
  public void testPushParser() {
    ColumnsStore columns = new MemColumnsStore(1024, new MemPageStore(), 800);
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    new GroupWriter(columnIO.getRecordWriter(columns), schema).write(r1);
    columns.flush();

    String[] expected = {
    "startMessage()",
     "startField(DocId, 0)",
      "addLong(10)",
     "endField(DocId, 0)",
     "startField(Links, 1)",
      "startGroup()",
       "startField(Forward, 1)",
        "addLong(20)",
        "addLong(40)",
        "addLong(60)",
       "endField(Forward, 1)",
      "endGroup()",
     "endField(Links, 1)",
     "startField(Name, 2)",
      "startGroup()",
       "startField(Language, 0)",
        "startGroup()",
         "startField(Code, 0)",
          "addBinary(en-us)",
         "endField(Code, 0)",
         "startField(Country, 1)",
          "addBinary(us)",
         "endField(Country, 1)",
        "endGroup()",
        "startGroup()",
         "startField(Code, 0)",
          "addBinary(en)",
         "endField(Code, 0)",
        "endGroup()",
       "endField(Language, 0)",
       "startField(Url, 1)",
        "addBinary(http://A)",
       "endField(Url, 1)",
      "endGroup()",
      "startGroup()",
       "startField(Url, 1)",
        "addBinary(http://B)",
       "endField(Url, 1)",
      "endGroup()",
      "startGroup()",
       "startField(Language, 0)",
        "startGroup()",
         "startField(Code, 0)",
          "addBinary(en-gb)",
         "endField(Code, 0)",
         "startField(Country, 1)",
          "addBinary(gb)",
         "endField(Country, 1)",
        "endGroup()",
       "endField(Language, 0)",
      "endGroup()",
     "endField(Name, 2)",
    "endMessage()"
    };
    final Deque<String> expectations = new ArrayDeque<String>();
    for (String string : expected) {
      expectations.add(string);
    }
    RecordReader<Void> recordReader = columnIO.getRecordReader(columns, new RecordMaterializer<Void>() {

      int count = 0;
      private void validate(String got) {
        assertEquals("event #"+count, expectations.pop(), got);
        ++count;
      }

      @Override
      public void startMessage() {
        validate("startMessage()");
      }

      @Override
      public void startGroup() {
        validate("startGroup()");
      }

      @Override
      public void startField(String field, int index) {
        validate("startField("+field+", "+index+")");
      }

      @Override
      public void endMessage() {
        validate("endMessage()");
      }

      @Override
      public void endGroup() {
        validate("endGroup()");
      }

      @Override
      public void endField(String field, int index) {
        validate("endField("+field+", "+index+")");
      }

      @Override
      public void addInteger(int value) {
        validate("addInt("+value+")");
      }

      @Override
      public void addLong(long value) {
        validate("addLong("+value+")");
      }

      @Override
      public void addBoolean(boolean value) {
        validate("addBoolean("+value+")");
      }

      @Override
      public void addBinary(byte[] value) {
        validate("addBinary("+new String(value)+")");
      }

      @Override
      public void addFloat(float value) {
        validate("addFloat("+value+")");
      }

      @Override
      public void addDouble(double value) {
        validate("addDouble("+value+")");
      }

      @Override
      public Void getCurrentRecord() {
        return null;
      }
    });
    recordReader.read();

  }

  public void read(RecordReader<Group> recordReader, MessageType schema, List<Group> records) {
    records.add(recordReader.read());
  }

  @Test
  public void testGroupWriter() {
    List<Group> result = new ArrayList<Group>();

    GroupRecordConsumer groupConsumer = new GroupRecordConsumer(new SimpleGroupFactory(schema));
    GroupWriter groupWriter = new GroupWriter(new RecordConsumerLoggingWrapper(groupConsumer), schema);
    groupWriter.write(r1);
    result.add(groupConsumer.getCurrentRecord());
    groupWriter.write(r2);
    result.add(groupConsumer.getCurrentRecord());
    assertEquals("deserialization does not display the expected result", result.get(0).toString(), r1.toString());
    assertEquals("deserialization does not display the expected result", result.get(1).toString(), r2.toString());
  }

  @Test
  public void testWriteWithGroupWriter() {

    final String[] expected = {
        "[DocId]: 10, r:0, d:0",
        "[Links, Backward]: null, r:0, d:1",
        "[Links, Forward]: 20, r:0, d:2",
        "[Links, Forward]: 40, r:1, d:2",
        "[Links, Forward]: 60, r:1, d:2",
        "[Name, Language, Code]: en-us, r:0, d:2",
        "[Name, Language, Country]: us, r:0, d:3",
        "[Name, Language, Code]: en, r:2, d:2",
        "[Name, Language, Country]: null, r:2, d:2",
        "[Name, Url]: http://A, r:0, d:2",
        "[Name, Language, Code]: null, r:1, d:1",
        "[Name, Language, Country]: null, r:1, d:1",
        "[Name, Url]: http://B, r:1, d:2",
        "[Name, Language, Code]: en-gb, r:1, d:2",
        "[Name, Language, Country]: gb, r:1, d:3",
        "[Name, Url]: null, r:1, d:1",
        "[DocId]: 20, r:0, d:0",
        "[Links, Backward]: 10, r:0, d:2",
        "[Links, Backward]: 30, r:1, d:2",
        "[Links, Forward]: 80, r:0, d:2",
        "[Name, Language, Code]: null, r:0, d:1",
        "[Name, Language, Country]: null, r:0, d:1",
        "[Name, Url]: http://C, r:0, d:2"
    };


    ColumnsStore columns = new ColumnsStore() {

      int counter = 0;

      @Override
      public ColumnWriter getColumnWriter(final ColumnDescriptor path) {
        return new ColumnWriter() {
          private void validate(Object value, int repetitionLevel,
              int definitionLevel) {
            String actual = Arrays.toString(path.getPath())+": "+value+", r:"+repetitionLevel+", d:"+definitionLevel;
            assertEquals("event #" + counter, expected[counter], actual);
            ++ counter;
          }

          @Override
          public void writeNull(int repetitionLevel, int definitionLevel) {
            validate(null, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(byte[] value, int repetitionLevel, int definitionLevel) {
            validate(new String(value), repetitionLevel, definitionLevel);
          }

          @Override
          public void write(boolean value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(int value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(long value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(float value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(double value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }

          @Override
          public void flush() {
            throw new UnsupportedOperationException();
          }

          @Override
          public long memSize() {
            throw new UnsupportedOperationException();
          }
        };
      }
      @Override
      public ColumnReader getColumnReader(ColumnDescriptor path) {
        throw new UnsupportedOperationException();
      }
      @Override
      public void flush() {
        assertEquals("read all events", expected.length, counter);
      }
    };
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), schema);
    groupWriter.write(r1);
    groupWriter.write(r2);
    columns.flush();
  }
}
