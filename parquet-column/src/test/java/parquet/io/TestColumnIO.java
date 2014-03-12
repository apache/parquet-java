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
import static org.junit.Assert.fail;
import static parquet.example.Paper.pr1;
import static parquet.example.Paper.pr2;
import static parquet.example.Paper.r1;
import static parquet.example.Paper.r2;
import static parquet.example.Paper.schema;
import static parquet.example.Paper.schema2;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REQUIRED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnWriteStore;
import parquet.column.ColumnWriter;
import parquet.column.ParquetProperties;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.column.page.PageReadStore;
import parquet.column.page.mem.MemPageStore;
import parquet.example.data.Group;
import parquet.example.data.GroupFactory;
import parquet.example.data.GroupWriter;
import parquet.example.data.simple.NanoTime;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

public class TestColumnIO {
  private static final Log LOG = Log.getLog(TestColumnIO.class);

  private static final String oneOfEach =
    "message Document {\n"
  + "  required int64 a;\n"
  + "  required int32 b;\n"
  + "  required float c;\n"
  + "  required double d;\n"
  + "  required boolean e;\n"
  + "  required binary f;\n"
  + "  required int96 g;\n"
  + "}\n";

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
  public void testReadUsingRequestedSchemaWithExtraFields(){
    MessageType orginalSchema = new MessageType("schema",
            new PrimitiveType(REQUIRED, INT32, "a"),
            new PrimitiveType(OPTIONAL, INT32, "b")
    );
    MessageType schemaWithExtraField = new MessageType("schema",
            new PrimitiveType(OPTIONAL, INT32, "b"),
            new PrimitiveType(OPTIONAL, INT32, "a"),
            new PrimitiveType(OPTIONAL, INT32, "c")
    );
    MemPageStore memPageStoreForOriginalSchema = new MemPageStore(1);
    MemPageStore memPageStoreForSchemaWithExtraField = new MemPageStore(1);
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(orginalSchema);
    writeGroups(orginalSchema, memPageStoreForOriginalSchema, groupFactory.newGroup().append("a", 1).append("b", 2));

    SimpleGroupFactory groupFactory2 = new SimpleGroupFactory(schemaWithExtraField);
    writeGroups(schemaWithExtraField, memPageStoreForSchemaWithExtraField, groupFactory2.newGroup().append("a", 1).append("b", 2).append("c",3));

    {
      List<Group> groups = new ArrayList<Group>();
      groups.addAll(readGroups(memPageStoreForOriginalSchema, orginalSchema, schemaWithExtraField, 1));
      groups.addAll(readGroups(memPageStoreForSchemaWithExtraField, schemaWithExtraField, schemaWithExtraField, 1));
      // TODO: add once we have the support for empty projection
//      groups1.addAll(readGroups(memPageStore3, schema3, schema2, 1));
      Object[][] expected = {
              { 2, 1, null},
              { 2, 1, 3},
//          { null, null}
      };
      validateGroups(groups, expected);
    }
  }

  @Test
  public void testReadUsingRequestedSchemaWithIncompatibleField(){
    MessageType originalSchema = new MessageType("schema",
            new PrimitiveType(OPTIONAL, INT32, "e"));
    MemPageStore store = new MemPageStore(1);
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(originalSchema);
    writeGroups(originalSchema, store, groupFactory.newGroup().append("e", 4));

    try {
      MessageType schemaWithIncompatibleField = new MessageType("schema",
              new PrimitiveType(OPTIONAL, BINARY, "e")); // Incompatible schema: different type
      readGroups(store, originalSchema, schemaWithIncompatibleField, 1);
      fail("should have thrown an incompatible schema exception");
    } catch (ParquetDecodingException e) {
      assertEquals("The requested schema is not compatible with the file schema. incompatible types: optional binary e != optional int32 e", e.getMessage());
    }
  }

  @Test
  public void testReadUsingSchemaWithRequiredFieldThatWasOptional(){
    MessageType originalSchema = new MessageType("schema",
            new PrimitiveType(OPTIONAL, INT32, "e"));
    MemPageStore store = new MemPageStore(1);
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(originalSchema);
    writeGroups(originalSchema, store, groupFactory.newGroup().append("e", 4));

    try {
      MessageType schemaWithRequiredFieldThatWasOptional = new MessageType("schema",
              new PrimitiveType(REQUIRED, INT32, "e")); // Incompatible schema: required when it was optional
      readGroups(store, originalSchema, schemaWithRequiredFieldThatWasOptional, 1);
      fail("should have thrown an incompatible schema exception");
    } catch (ParquetDecodingException e) {
      assertEquals("The requested schema is not compatible with the file schema. incompatible types: required int32 e != optional int32 e", e.getMessage());
    }
  }

  @Test
  public void testReadUsingProjectedSchema(){
    MessageType orginalSchema = new MessageType("schema",
            new PrimitiveType(REQUIRED, INT32, "a"),
            new PrimitiveType(REQUIRED, INT32, "b")
    );
    MessageType projectedSchema = new MessageType("schema",
            new PrimitiveType(OPTIONAL, INT32, "b")
    );
    MemPageStore store = new MemPageStore(1);
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(orginalSchema);
    writeGroups(orginalSchema, store, groupFactory.newGroup().append("a", 1).append("b", 2));

    {
      List<Group> groups = new ArrayList<Group>();
      groups.addAll(readGroups(store, orginalSchema, projectedSchema, 1));
      Object[][] expected = {
              {2},
      };
      validateGroups(groups, expected);
    }
  }

  private void validateGroups(List<Group> groups1, Object[][] e1) {
    Iterator<Group> i1 = groups1.iterator();
    for (int i = 0; i < e1.length; i++) {
      Object[] objects = e1[i];
      Group next = i1.next();
      for (int j = 0; j < objects.length; j++) {
        Object object = objects[j];
        if (object == null) {
          assertEquals(0, next.getFieldRepetitionCount(j));
        } else {
          assertEquals("looking for r[" + i + "][" + j + "][0]=" + object, 1, next.getFieldRepetitionCount(j));
          assertEquals(object, next.getInteger(j, 0));
        }
      }
    }
  }

  private List<Group> readGroups(MemPageStore memPageStore, MessageType fileSchema, MessageType requestedSchema, int n) {
    ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
    MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema);
    RecordReaderImplementation<Group> recordReader = getRecordReader(columnIO, requestedSchema, memPageStore);
    List<Group> groups = new ArrayList<Group>();
    for (int i = 0; i < n; i++) {
      groups.add(recordReader.read());
    }
    return groups;
  }

  private void writeGroups(MessageType writtenSchema, MemPageStore memPageStore, Group... groups) {
    ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
    ColumnWriteStoreImpl columns = newColumnWriteStore(memPageStore);
    MessageColumnIO columnIO = columnIOFactory.getColumnIO(writtenSchema);
    GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(columns), writtenSchema);
    for (Group group : groups) {
      groupWriter.write(group);
    }
    columns.flush();
  }

  @Test
  public void testColumnIO() {
    log(schema);
    log("r1");
    log(r1);
    log("r2");
    log(r2);

    MemPageStore memPageStore = new MemPageStore(2);
    ColumnWriteStoreImpl columns = newColumnWriteStore(memPageStore);

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
      records.add(recordReader.read());
      records.add(recordReader.read());

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

      records.add(recordReader.read());
      records.add(recordReader.read());

      int i = 0;
      for (Group record : records) {
        log("r" + (++i));
        log(record);
      }
      assertEquals("deserialization does not display the expected result", pr1.toString(), records.get(0).toString());
      assertEquals("deserialization does not display the expected result", pr2.toString(), records.get(1).toString());
    }
  }

  @Test
  public void testOneOfEach() {
    MessageType oneOfEachSchema = MessageTypeParser.parseMessageType(oneOfEach);
    GroupFactory gf = new SimpleGroupFactory(oneOfEachSchema);
    Group g1 = gf.newGroup()
        .append("a", 1l)
        .append("b", 2)
        .append("c", 3.0f)
        .append("d", 4.0d)
        .append("e", true)
        .append("f", Binary.fromString("6"))
        .append("g", new NanoTime(1234, System.currentTimeMillis() * 1000));

    testSchema(oneOfEachSchema, Arrays.asList(g1));
  }

  @Test
  public void testRequiredOfRequired() {
    MessageType reqreqSchema = MessageTypeParser.parseMessageType(
          "message Document {\n"
        + "  required group foo {\n"
        + "    required int64 bar;\n"
        + "  }\n"
        + "}\n");

    GroupFactory gf = new SimpleGroupFactory(reqreqSchema);
    Group g1 = gf.newGroup();
    g1.addGroup("foo").append("bar", 2l);

    testSchema(reqreqSchema, Arrays.asList(g1));
  }

  @Test
  public void testOptionalRequiredInteraction() {
    for (int i = 0; i < 6; i++) {
      Type current = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "primitive");
      for (int j = 0; j < i; j++) {
        current = new GroupType(Repetition.REQUIRED, "req" + j, current);
      }
      MessageType groupSchema = new MessageType("schema"+i, current);
      GroupFactory gf = new SimpleGroupFactory(groupSchema);
      List<Group> groups = new ArrayList<Group>();
      Group root = gf.newGroup();
      Group currentGroup = root;
      for (int j = 0; j < i; j++) {
        currentGroup = currentGroup.addGroup(0);
      }
      currentGroup.add(0, Binary.fromString("foo"));
      groups.add(root);
      testSchema(groupSchema, groups);
    }
    for (int i = 0; i < 6; i++) {
      Type current = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "primitive");
      for (int j = 0; j < i; j++) {
        current = new GroupType(Repetition.REQUIRED, "req" + j, current);
      }
      MessageType groupSchema = new MessageType("schema"+(i+6), current);
      GroupFactory gf = new SimpleGroupFactory(groupSchema);
      List<Group> groups = new ArrayList<Group>();
      Group rootDefined = gf.newGroup();
      Group rootUndefined = gf.newGroup();
      Group currentDefinedGroup = rootDefined;
      Group currentUndefinedGroup = rootUndefined;
      for (int j = 0; j < i; j++) {
        currentDefinedGroup = currentDefinedGroup.addGroup(0);
        currentUndefinedGroup = currentUndefinedGroup.addGroup(0);
      }
      currentDefinedGroup.add(0, Binary.fromString("foo"));
      groups.add(rootDefined);
      groups.add(rootUndefined);
      testSchema(groupSchema, groups);
    }
    for (int i = 0; i < 6; i++) {
      Type current = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "primitive");
      for (int j = 0; j < 6; j++) {
        current = new GroupType(i==j ? Repetition.OPTIONAL : Repetition.REQUIRED, "req" + j, current);
      }
      MessageType groupSchema = new MessageType("schema"+(i+12), current);
      GroupFactory gf = new SimpleGroupFactory(groupSchema);
      List<Group> groups = new ArrayList<Group>();
      Group rootDefined = gf.newGroup();
      Group rootUndefined = gf.newGroup();
      Group currentDefinedGroup = rootDefined;
      Group currentUndefinedGroup = rootUndefined;
      for (int j = 0; j < 6; j++) {
        currentDefinedGroup = currentDefinedGroup.addGroup(0);
        if (i < j) {
          currentUndefinedGroup = currentUndefinedGroup.addGroup(0);
        }
      }
      currentDefinedGroup.add(0, Binary.fromString("foo"));
      groups.add(rootDefined);
      groups.add(rootUndefined);
      testSchema(groupSchema, groups);
    }
  }

  private void testSchema(MessageType messageSchema, List<Group> groups) {
    MemPageStore memPageStore = new MemPageStore(groups.size());
    ColumnWriteStoreImpl columns = newColumnWriteStore(memPageStore);

    ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
    MessageColumnIO columnIO = columnIOFactory.getColumnIO(messageSchema);
    log(columnIO);

    // Write groups.
    GroupWriter groupWriter =
        new GroupWriter(columnIO.getRecordWriter(columns), messageSchema);
    for (Group group : groups) {
      groupWriter.write(group);
    }
    columns.flush();

    // Read groups and verify.
    RecordReaderImplementation<Group> recordReader =
        getRecordReader(columnIO, messageSchema, memPageStore);
    for (Group group : groups) {
      final Group got = recordReader.read();
      assertEquals("deserialization does not display the same result",
                   group.toString(), got.toString());
    }
  }

  private RecordReaderImplementation<Group> getRecordReader(MessageColumnIO columnIO, MessageType schema, PageReadStore pageReadStore) {
    RecordMaterializer<Group> recordConverter = new GroupRecordConverter(schema);

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
    MemPageStore memPageStore = new MemPageStore(1);
    ColumnWriteStoreImpl columns = newColumnWriteStore(memPageStore);
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    new GroupWriter(columnIO.getRecordWriter(columns), schema).write(r1);
    columns.flush();

    RecordReader<Void> recordReader = columnIO.getRecordReader(memPageStore, new ExpectationValidatingConverter(expectedEventsForR1, schema));
    recordReader.read();

  }

  private ColumnWriteStoreImpl newColumnWriteStore(MemPageStore memPageStore) {
    return new ColumnWriteStoreImpl(memPageStore, 800, new ParquetProperties());
  }

  @Test
  public void testEmptyField() {
    MemPageStore memPageStore = new MemPageStore(1);
    ColumnWriteStoreImpl columns = newColumnWriteStore(memPageStore);
    MessageColumnIO columnIO = new ColumnIOFactory(true).getColumnIO(schema);
    final RecordConsumer recordWriter = columnIO.getRecordWriter(columns);
    recordWriter.startMessage();
    recordWriter.startField("DocId", 0);
    recordWriter.addLong(0);
    recordWriter.endField("DocId", 0);
    recordWriter.startField("Links", 1);
    try {
      recordWriter.endField("Links", 1);
      Assert.fail("expected exception because of empty field");
    } catch (ParquetEncodingException e) {
      Assert.assertEquals("empty fields are illegal, the field should be ommited completely instead", e.getMessage());
    }
  }

  @Test
  public void testGroupWriter() {
    List<Group> result = new ArrayList<Group>();
    final GroupRecordConverter groupRecordConverter = new GroupRecordConverter(schema);
    RecordConsumer groupConsumer = new ConverterConsumer(groupRecordConverter.getRootConverter(), schema);
    GroupWriter groupWriter = new GroupWriter(new RecordConsumerLoggingWrapper(groupConsumer), schema);
    groupWriter.write(r1);
    result.add(groupRecordConverter.getCurrentRecord());
    groupWriter.write(r2);
    result.add(groupRecordConverter.getCurrentRecord());
    assertEquals("deserialization does not display the expected result", result.get(0).toString(), r1.toString());
    assertEquals("deserialization does not display the expected result", result.get(1).toString(), r2.toString());
  }

  @Test
  public void testWriteWithGroupWriter() {

    final String[] expected = {
        "[DocId]: 10, r:0, d:0",
        "[Links, Forward]: 20, r:0, d:2",
        "[Links, Forward]: 40, r:1, d:2",
        "[Links, Forward]: 60, r:1, d:2",
        "[Links, Backward]: null, r:0, d:1",
        "[Name, Language, Code]: en-us, r:0, d:2",
        "[Name, Language, Country]: us, r:0, d:3",
        "[Name, Language, Code]: en, r:2, d:2",
        "[Name, Language, Country]: null, r:2, d:2",
        "[Name, Url]: http://A, r:0, d:2",
        "[Name, Url]: http://B, r:1, d:2",
        "[Name, Language, Code]: null, r:1, d:1",
        "[Name, Language, Country]: null, r:1, d:1",
        "[Name, Language, Code]: en-gb, r:1, d:2",
        "[Name, Language, Country]: gb, r:1, d:3",
        "[Name, Url]: null, r:1, d:1",
        "[DocId]: 20, r:0, d:0",
        "[Links, Backward]: 10, r:0, d:2",
        "[Links, Backward]: 30, r:1, d:2",
        "[Links, Forward]: 80, r:0, d:2",
        "[Name, Url]: http://C, r:0, d:2",
        "[Name, Language, Code]: null, r:0, d:1",
        "[Name, Language, Country]: null, r:0, d:1"

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
