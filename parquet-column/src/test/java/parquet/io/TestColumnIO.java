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

import static org.junit.Assert.assertEquals;
import static parquet.example.Paper.pr1;
import static parquet.example.Paper.pr2;
import static parquet.example.Paper.r1;
import static parquet.example.Paper.r2;
import static parquet.example.Paper.schema;
import static parquet.example.Paper.schema2;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import org.junit.Test;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnWriteStore;
import parquet.column.ColumnWriter;
import parquet.column.mem.MemColumnWriteStore;
import parquet.column.mem.MemPageStore;
import parquet.column.mem.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.GroupRecordConsumer;
import parquet.example.data.GroupWriter;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.MessageType;


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

  public static final String[] expectedEventsForR1 = {
    "startMessage()",
    "DocId.addLong(10)",
    "Links.start()",
    "Links.Forward.addLong(20)",
    "Links.Forward.addLong(40)",
    "Links.Forward.addLong(60)",
    "Links.end()",
    "Name.start()",
    "Name.Language.start()",
    "Name.Language.Code.addBinary(en-us)",
    "Name.Language.Country.addBinary(us)",
    "Name.Language.end()",
    "Name.Language.start()",
    "Name.Language.Code.addBinary(en)",
    "Name.Language.end()",
    "Name.Url.addBinary(http://A)",
    "Name.end()",
    "Name.start()",
    "Name.Url.addBinary(http://B)",
    "Name.end()",
    "Name.start()",
    "Name.Language.start()",
    "Name.Language.Code.addBinary(en-gb)",
    "Name.Language.Country.addBinary(gb)",
    "Name.Language.end()",
    "Name.end()",
    "endMessage()"
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

    MemPageStore memPageStore = new MemPageStore();
    MemColumnWriteStore columns = new MemColumnWriteStore(memPageStore, 800);

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

      RecordReaderImplementation<Group> recordReader = getRecordReader(columnIO, schema, memPageStore);

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
      RecordReaderImplementation<Group> recordReader = getRecordReader(columnIO2, schema2, memPageStore);

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

  private RecordReaderImplementation<Group> getRecordReader(MessageColumnIO columnIO, MessageType schema, PageReadStore pageReadStore) {
    RecordConverter<Group> recordConverter = new GroupRecordConverter(schema);

    return (RecordReaderImplementation<Group>)columnIO.getRecordReader(pageReadStore, recordConverter);
  }

  private void log(Object o) {
    LOG.info(o);
  }

  private void validateFSA(int[][] expectedFSA, MessageColumnIO columnIO, RecordReaderImplementation<?> recordReader) {
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
    MemPageStore memPageStore = new MemPageStore();
    MemColumnWriteStore columns = new MemColumnWriteStore(memPageStore, 800);
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    new GroupWriter(columnIO.getRecordWriter(columns), schema).write(r1);
    columns.flush();

    final Deque<String> expectations = new ArrayDeque<String>();
    for (String string : expectedEventsForR1) {
      expectations.add(string);
    }

    RecordReader<Void> recordReader = columnIO.getRecordReader(memPageStore, new ExpectationValidatingConverter(expectations, schema));
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


    ColumnWriteStore columns = new ColumnWriteStore() {

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
          public void write(Binary value, int repetitionLevel, int definitionLevel) {
            validate(value.toStringUsingUTF8(), repetitionLevel, definitionLevel);
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
          public long getBufferedSizeInMemory() {
            throw new UnsupportedOperationException();
          }
        };
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
