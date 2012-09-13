package redelm.io;

import static redelm.schema.PrimitiveType.Primitive.INT64;
import static redelm.schema.PrimitiveType.Primitive.STRING;
import static redelm.schema.Type.Repetition.OPTIONAL;
import static redelm.schema.Type.Repetition.REPEATED;
import static redelm.schema.Type.Repetition.REQUIRED;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import junit.framework.Assert;
import redelm.Log;
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.column.ColumnsStore;
import redelm.column.mem.MemColumnsStore;
import redelm.data.Group;
import redelm.data.GroupRecordConsumer;
import redelm.data.GroupWriter;
import redelm.data.simple.SimpleGroup;
import redelm.data.simple.SimpleGroupFactory;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;

import org.junit.Test;

public class TestColumnIO {
  private static final String schemaString =
      "message Document {\n"
    + "  required int64 DocId;\n"
    + "  optional group Links {\n"
    + "    repeated int64 Backward;\n"
    + "    repeated int64 Forward;\n"
    + "  };\n"
    + "  repeated group Name {\n"
    + "    repeated group Language {\n"
    + "      required string Code;\n"
    + "      optional string Country;\n"
    + "    };\n"
    + "    optional string Url;\n"
    + "  };\n"
    + "}\n";

  public static final MessageType schema =
      new MessageType("Document",
          new PrimitiveType(REQUIRED, INT64, "DocId"),
          new GroupType(OPTIONAL, "Links",
              new PrimitiveType(REPEATED, INT64, "Backward"),
              new PrimitiveType(REPEATED, INT64, "Forward")
              ),
          new GroupType(REPEATED, "Name",
              new GroupType(REPEATED, "Language",
                  new PrimitiveType(REQUIRED, STRING, "Code"),
                  new PrimitiveType(OPTIONAL, STRING, "Country")),
              new PrimitiveType(OPTIONAL, STRING, "Url")));

  public static final MessageType schema2 =
      new MessageType("Document",
          new PrimitiveType(REQUIRED, INT64, "DocId"),
          new GroupType(REPEATED, "Name",
              new GroupType(REPEATED, "Language",
                  new PrimitiveType(OPTIONAL, STRING, "Country"))));

  public static final MessageType schema3 =
      new MessageType("Document",
          new PrimitiveType(REQUIRED, INT64, "DocId"),
          new GroupType(OPTIONAL, "Links",
              new PrimitiveType(REPEATED, INT64, "Backward"),
              new PrimitiveType(REPEATED, INT64, "Forward")
              ));

  public static final SimpleGroup r1 = new SimpleGroup(schema);
  public static final SimpleGroup r2 = new SimpleGroup(schema);
  ////r1
  //DocId: 10
  //Links
  //  Forward: 20
  //  Forward: 40
  //  Forward: 60
  //Name
  //  Language
  //    Code: 'en-us'
  //    Country: 'us'
  //  Language
  //    Code: 'en'
  //  Url: 'http://A'
  //Name
  //  Url: 'http://B'
  //Name
  //  Language
  //    Code: 'en-gb'
  //    Country: 'gb'
    static {
      r1.add("DocId", 10);
      r1.addGroup("Links")
        .append("Forward", 20)
        .append("Forward", 40)
        .append("Forward", 60);
      Group name = r1.addGroup("Name");
      {
        name.addGroup("Language")
          .append("Code", "en-us")
          .append("Country", "us");
        name.addGroup("Language")
          .append("Code", "en");
        name.append("Url", "http://A");
      }
      name = r1.addGroup("Name");
      {
        name.append("Url", "http://B");
      }
      name = r1.addGroup("Name");
      {
        name.addGroup("Language")
          .append("Code", "en-gb")
          .append("Country", "gb");
      }
    }
////r2
//DocId: 20
//Links
// Backward: 10
// Backward: 30
// Forward:  80
//Name
// Url: 'http://C'
 static {
   r2.add("DocId", 20);
   r2.addGroup("Links")
     .append("Backward", 10)
     .append("Backward", 30)
     .append("Forward", 80);
   r2.addGroup("Name")
     .append("Url", "http://C");
 }

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

 private static final SimpleGroup pr1 = new SimpleGroup(schema2);
 private static final SimpleGroup pr2 = new SimpleGroup(schema2);
 ////r1
 //DocId: 10
 //Name
 //  Language
 //    Country: 'us'
 //  Language
 //Name
 //Name
 //  Language
 //    Country: 'gb'
   static {
     pr1.add("DocId", 10);
     Group name = pr1.addGroup("Name");
     {
       name.addGroup("Language")
         .append("Country", "us");
       name.addGroup("Language");
     }
     name = pr1.addGroup("Name");
     name = pr1.addGroup("Name");
     {
       name.addGroup("Language")
         .append("Country", "gb");
     }
   }

////r2
//DocId: 20
//Name
   static {
     pr2.add("DocId", 20);
     pr2.addGroup("Name");
   }

   @Test
   public void testSchema() {
     Assert.assertEquals(schemaString, schema.toString());
   }

  @Test
  public void testColumnIO() {
    System.out.println(schema);
    System.out.println("r1");
    System.out.println(r1);
    System.out.println("r2");
    System.out.println(r2);

    ColumnsStore columns = new MemColumnsStore(1024);
    {
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema, columns);
      System.out.println(columnIO);
      GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(), schema);
      groupWriter.write(r1);
      groupWriter.write(r2);
      System.out.println(columns);
      System.out.println("=========");

      List<Group> records = new ArrayList<Group>();
      RecordReader recordReader = columnIO.getRecordReader();

      validateFSA(expectedFSA, columnIO, recordReader);

      read(recordReader, schema, records);
      read(recordReader, schema, records);

      int i = 0;
      for (Group record : records) {
        System.out.println("r" + (++i));
        System.out.println(record);
      }

      Assert.assertEquals("deserialization does not display the same result", records.get(0).toString(), r1.toString());
      Assert.assertEquals("deserialization does not display the same result", records.get(1).toString(), r2.toString());

    }
    {
      MessageColumnIO columnIO2 = new ColumnIOFactory().getColumnIO(schema2, columns);
      List<Group> records = new ArrayList<Group>();
      RecordReader recordReader = columnIO2.getRecordReader();

      validateFSA(expectedFSA2, columnIO2, recordReader);

      read(recordReader, schema2, records);
      read(recordReader, schema2, records);

      int i = 0;
      for (Group record : records) {
        System.out.println("r" + (++i));
        System.out.println(record);
      }
      Assert.assertEquals("deserialization does not display the expected result", records.get(0).toString(), pr1.toString());
      Assert.assertEquals("deserialization does not display the expected result", records.get(1).toString(), pr2.toString());

    }
  }

  private void validateFSA(int[][] expectedFSA, MessageColumnIO columnIO, RecordReader recordReader) {
    System.out.println("FSA: ----");
    List<PrimitiveColumnIO> leaves = columnIO.getLeaves();
    for (int i = 0; i < leaves.size(); ++i) {
      PrimitiveColumnIO primitiveColumnIO = leaves.get(i);
      System.out.println(Arrays.toString(primitiveColumnIO.getFieldPath()));
      for (int r = 0; r < expectedFSA[i].length; r++) {
        int next = expectedFSA[i][r];
        System.out.println(" "+r+" -> "+ (next==leaves.size() ? "end" : Arrays.toString(leaves.get(next).getFieldPath()))+": "+recordReader.getNextLevel(i, r));
        Assert.assertEquals(Arrays.toString(primitiveColumnIO.getFieldPath())+": "+r+" -> ", next, recordReader.getNextReader(i, r));
      }
    }
    System.out.println("----");
  }

  @Test
  public void testPushParser() {
    ColumnsStore columns = new MemColumnsStore(1024);
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema, columns);
    new GroupWriter(columnIO.getRecordWriter(), schema).write(r1);
    RecordReader recordReader = columnIO.getRecordReader();

    String[] expected = {
    "startMessage()",
     "startField(DocId, 0)",
      "addInt(10)",
     "endField(DocId, 0)",
     "startField(Links, 1)",
      "startGroup()",
       "startField(Forward, 1)",
        "addInt(20)",
        "addInt(40)",
        "addInt(60)",
       "endField(Forward, 1)",
      "endGroup()",
     "endField(Links, 1)",
     "startField(Name, 2)",
      "startGroup()",
       "startField(Language, 0)",
        "startGroup()",
         "startField(Code, 0)",
          "addString(en-us)",
         "endField(Code, 0)",
         "startField(Country, 1)",
          "addString(us)",
         "endField(Country, 1)",
        "endGroup()",
        "startGroup()",
         "startField(Code, 0)",
          "addString(en)",
         "endField(Code, 0)",
        "endGroup()",
       "endField(Language, 0)",
       "startField(Url, 1)",
        "addString(http://A)",
       "endField(Url, 1)",
      "endGroup()",
      "startGroup()",
       "startField(Url, 1)",
        "addString(http://B)",
       "endField(Url, 1)",
      "endGroup()",
      "startGroup()",
       "startField(Language, 0)",
        "startGroup()",
         "startField(Code, 0)",
          "addString(en-gb)",
         "endField(Code, 0)",
         "startField(Country, 1)",
          "addString(gb)",
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

    recordReader.read(new RecordConsumer() {

      int count = 0;
      private void validate(String got) {
        Assert.assertEquals("event #"+count, expectations.pop(), got);
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
      public void addString(String value) {
        validate("addString("+value+")");
      }

      @Override
      public void addInt(int value) {
        validate("addInt("+value+")");
      }

      @Override
      public void addBoolean(boolean value) {
        validate("addBoolean("+value+")");
      }

      @Override
      public void addBinary(byte[] value) {
        validate("addBinary("+new BigInteger(value).toString(16)+")");
      }
    });

  }

  public void read(RecordReader recordReader, MessageType schema, List<Group> groups) {
    RecordConsumer recordConsumer = new GroupRecordConsumer(new SimpleGroupFactory(schema), groups);
    if (Log.DEBUG) {
      recordConsumer = new RecordConsumerWrapper(recordConsumer);
    }
    recordReader.read(recordConsumer);
  }

  @Test
  public void testGroupWriter() {
    List<Group> result = new ArrayList<Group>();
    GroupWriter groupWriter = new GroupWriter(new RecordConsumerWrapper(new GroupRecordConsumer(new SimpleGroupFactory(schema), result)), schema);
    groupWriter.write(r1);
    groupWriter.write(r2);
    Assert.assertEquals("deserialization does not display the expected result", result.get(0).toString(), r1.toString());
    Assert.assertEquals("deserialization does not display the expected result", result.get(1).toString(), r2.toString());
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
            Assert.assertEquals("event #" + counter, expected[counter], actual);
            ++ counter;
          }

          @Override
          public void writeNull(int repetitionLevel, int definitionLevel) {
            validate(null, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(byte[] value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(boolean value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(String value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }

          @Override
          public void write(int value, int repetitionLevel, int definitionLevel) {
            validate(value, repetitionLevel, definitionLevel);
          }
        };
      }
      @Override
      public Collection<ColumnReader> getColumnReaders() {
        throw new UnsupportedOperationException();
      }
      @Override
      public ColumnReader getColumnReader(ColumnDescriptor path) {
        throw new UnsupportedOperationException();
      }
    };
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema, columns);
    GroupWriter groupWriter = new GroupWriter(columnIO.getRecordWriter(), schema);
    groupWriter.write(r1);
    groupWriter.write(r2);
  }
}
