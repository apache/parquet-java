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
package parquet.pig;

import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static parquet.pig.TupleReadSupport.PARQUET_PIG_SCHEMA;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.junit.Test;

import parquet.Log;
import parquet.example.data.Group;
import parquet.example.data.GroupWriter;
import parquet.example.data.simple.SimpleGroup;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.io.ConverterConsumer;
import parquet.io.RecordConsumerLoggingWrapper;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

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
    RecordMaterializer<Tuple> recordConsumer = newPigRecordConsumer(pigSchemaString);
    TupleWriteSupport tupleWriter = newTupleWriter(pigSchemaString, recordConsumer);
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
    RecordMaterializer<Tuple> pigRecordConsumer = newPigRecordConsumer(pigSchemaString);
    GroupWriter groupWriter = new GroupWriter(new RecordConsumerLoggingWrapper(new ConverterConsumer(pigRecordConsumer.getRootConverter(), schema)), schema);

    for (Group group : input) {
      groupWriter.write(group);
      final Tuple tuple = pigRecordConsumer.getCurrentRecord();
      tuples.add(tuple);
      logger.debug("in: "+group+"\nout:"+tuple);
    }

    List<Group> groups = new ArrayList<Group>();
    GroupRecordConverter recordConsumer = new GroupRecordConverter(schema);
    TupleWriteSupport tupleWriter = newTupleWriter(pigSchemaString, recordConsumer);
    for (Tuple t : tuples) {
      logger.debug(t);
      tupleWriter.write(t);
      groups.add(recordConsumer.getCurrentRecord());
    }

    assertEquals(input.size(), groups.size());
    for (int i = 0; i < input.size(); i++) {
      Group in = input.get(i);
      logger.debug(in);
      Group out = groups.get(i);
      assertEquals(in.toString(), out.toString());
    }
  }

  private <T> TupleWriteSupport newTupleWriter(String pigSchemaString, RecordMaterializer<T> recordConsumer) throws ParserException {
    TupleWriteSupport tupleWriter = TupleWriteSupport.fromPigSchema(pigSchemaString);
    tupleWriter.init(null);
    tupleWriter.prepareForWrite(
        new ConverterConsumer(recordConsumer.getRootConverter(), tupleWriter.getParquetSchema())
        );
    return tupleWriter;
  }

  private Map<String, String> pigMetaData(String pigSchemaString) {
    Map<String, String> map = new HashMap<String, String>();
    new PigMetaData(pigSchemaString).addToMetaData(map);
    return map;
  }

  private RecordMaterializer<Tuple> newPigRecordConsumer(String pigSchemaString) throws ParserException {
    TupleReadSupport tupleReadSupport = new TupleReadSupport();
    final Configuration configuration = new Configuration(false);
    MessageType parquetSchema = getMessageType(pigSchemaString);
    final Map<String, String> pigMetaData = pigMetaData(pigSchemaString);
    Map<String, Set<String>> globalMetaData = new HashMap<String, Set<String>>();
    for (Entry<String, String> entry : pigMetaData.entrySet()) {
      globalMetaData.put(entry.getKey(), new HashSet<String>(Arrays.asList(entry.getValue())));
    }
    configuration.set(PARQUET_PIG_SCHEMA, pigSchemaString);
    final ReadContext init = tupleReadSupport.init(new InitContext(configuration, globalMetaData, parquetSchema));
    return tupleReadSupport.prepareForRead(configuration, pigMetaData, parquetSchema, init);
  }

  private MessageType getMessageType(String pigSchemaString) throws ParserException {
    Schema pigSchema = Utils.getSchemaFromString(pigSchemaString);
    return new PigSchemaConverter().convert(pigSchema);
  }

}
