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

import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redelm.Log;
import redelm.data.Group;
import redelm.data.GroupRecordConsumer;
import redelm.data.GroupWriter;
import redelm.data.simple.SimpleGroup;
import redelm.data.simple.SimpleGroupFactory;
import redelm.io.RecordMaterializer;
import redelm.schema.MessageType;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.junit.Test;

public class TestTupleRecordConsumer {
  private static final Log logger = Log.getLog(TestTupleRecordConsumer.class);

  @Test
  public void testArtSchema() throws ExecException, ParserException {

    String pigSchemaString =
            "DocId:long, " +
            "Links:(Backward:{(long)}, Forward:{(long)}), " +
            "Name:{(Language:{(Code:chararray,Country:chararray)}, Url:chararray)}";

    SimpleGroup g = new SimpleGroup(getMessageType(pigSchemaString));
    g.add("DocId", 1l);
    Group links = g.addGroup("Links");
    links.addGroup("Backward").addGroup("bag").add(0, 1l);
    links.addGroup("Forward").addGroup("bag").add(0, 1l);
    Group name = g.addGroup("Name").addGroup("bag");
    name.addGroup("Language").addGroup("bag").append("Code", "en").append("Country", "US");
    name.add("Url", "http://foo/bar");

    testFromGroups(pigSchemaString, Arrays.<Group>asList(g));
  }

  @Test
  public void testBags() throws ExecException, ParserException {
    String pigSchemaString = "a: {(b: chararray)}";

    SimpleGroup g = new SimpleGroup(getMessageType(pigSchemaString));
    Group addGroup = g.addGroup("a");
    addGroup.addGroup("bag").append("b", "foo");
    addGroup.addGroup("bag").append("b", "bar");

    testFromGroups(pigSchemaString, Arrays.<Group>asList(g));
  }

  @Test
  public void testMaps() throws ExecException, ParserException {
        String pigSchemaString = "a: [(b: chararray)]";
    SimpleGroup g = new SimpleGroup(getMessageType(pigSchemaString));
    Group map = g.addGroup("a");
    map.addGroup("map").append("key", "foo").addGroup("value").append("b", "foo");
    map.addGroup("map").append("key", "bar").addGroup("value").append("b", "bar");

    testFromGroups(pigSchemaString, Arrays.<Group>asList(g));
  }

  @Test
  public void testComplexSchema() throws Exception {

    String pigSchemaString = "a:chararray, b:{t:(c:chararray, d:chararray)}";
    Tuple t0 = tuple("a"+0, bag(tuple("o", "b"), tuple("o1", "b1")));
    Tuple t1 = tuple("a"+1, bag(tuple("o", "b"), tuple("o", "b"), tuple("o", "b"), tuple("o", "b")));
    Tuple t2 = tuple("a"+2, bag(tuple("o", "b"), tuple("o", null), tuple(null, "b"), tuple(null, null)));
    Tuple t3 = tuple("a"+3, null);
    testFromTuple(pigSchemaString, Arrays.asList(t0, t1, t2, t3));

  }

  @Test
  public void testMapSchema() throws Exception {

    String pigSchemaString = "a:chararray, b:[(c:chararray, d:chararray)]";
    Tuple t0 = tuple("a"+0, new HashMap() {{put("foo", tuple("o", "b"));}});
    Tuple t1 = tuple("a"+1, new HashMap() {{put("foo", tuple("o", "b")); put("foo", tuple("o", "b")); put("foo", tuple("o", "b")); put("foo", tuple("o", "b"));}});
    Tuple t2 = tuple("a"+2, new HashMap() {{put("foo", tuple("o", "b")); put("foo", tuple("o", null)); put("foo", tuple(null, "b")); put("foo", tuple(null, null));}});
    Tuple t3 = tuple("a"+3, null);
    testFromTuple(pigSchemaString, Arrays.asList(t0, t1, t2, t3));

  }

  private void testFromTuple(String pigSchemaString, List<Tuple> input) throws Exception {
    List<Tuple> tuples = new ArrayList<Tuple>();
    MessageType redelmSchema = getMessageType(pigSchemaString);
    RecordMaterializer<Tuple> recordConsumer = newPigRecordConsumer(redelmSchema, pigSchemaString);
    TupleWriteSupport tupleWriter = newTupleWriter(redelmSchema, pigSchemaString, recordConsumer);
    for (Tuple tuple : input) {
      logger.debug(tuple);
      tupleWriter.write(tuple);
      tuples.add(recordConsumer.getCurrentRecord());
    }

    assertEquals(input.size(), tuples.size());
    for (int i = 0; i < input.size(); i++) {
      Tuple in = input.get(i);
      Tuple out = tuples.get(i);
      assertEquals(in.toString(), out.toString());
    }

  }

  private void testFromGroups(String pigSchemaString, List<Group> input) throws ParserException {
    List<Tuple> tuples = new ArrayList<Tuple>();
    MessageType schema = getMessageType(pigSchemaString);
    RecordMaterializer<Tuple> pigRecordConsumer = newPigRecordConsumer(schema, pigSchemaString);
    GroupWriter groupWriter = new GroupWriter(pigRecordConsumer, schema);

    for (Group group : input) {
      groupWriter.write(group);
      tuples.add(pigRecordConsumer.getCurrentRecord());
    }

    List<Group> groups = new ArrayList<Group>();
    GroupRecordConsumer recordConsumer = new GroupRecordConsumer(new SimpleGroupFactory(schema));
    TupleWriteSupport tupleWriter = newTupleWriter(schema, pigSchemaString, recordConsumer);
    for (Tuple t : tuples) {
      logger.debug(t);
      tupleWriter.write(t);
      groups.add(recordConsumer.getCurrentRecord());
    }

    assertEquals(input.size(), groups.size());
    for (int i = 0; i < input.size(); i++) {
      Group in = input.get(i);
      Group out = groups.get(i);
      assertEquals(in.toString(), out.toString());
    }
  }

  private <T> TupleWriteSupport newTupleWriter(MessageType redelmSchema, String pigSchemaString, RecordMaterializer<T> recordConsumer) {
    TupleWriteSupport tupleWriter = new TupleWriteSupport();
    tupleWriter.initForWrite(
        recordConsumer,
        redelmSchema,
        pigMetaData(pigSchemaString)
        );
    return tupleWriter;
  }

  private Map<String, String> pigMetaData(String pigSchemaString) {
    Map<String, String> map = new HashMap<String, String>();
    new PigMetaData(pigSchemaString).addToMetaData(map);
    return map;
  }

  private RecordMaterializer<Tuple> newPigRecordConsumer(MessageType redelmSchema, String pigSchemaString) throws ParserException {
    TupleReadSupport tupleReadSupport = new TupleReadSupport();
    tupleReadSupport.initForRead(pigMetaData(pigSchemaString), redelmSchema.toString());
    return tupleReadSupport.newRecordConsumer();
  }

  private MessageType getMessageType(String pigSchemaString) throws ParserException {
    Schema pigSchema = Utils.getSchemaFromString(pigSchemaString);
    return new PigSchemaConverter().convert(pigSchema);
  }

}
