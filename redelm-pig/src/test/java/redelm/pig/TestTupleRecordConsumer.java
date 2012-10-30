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

import static org.junit.Assert.assertEquals;
import static redelm.data.simple.example.Paper.r1;
import static redelm.data.simple.example.Paper.r2;
import static redelm.data.simple.example.Paper.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import redelm.Log;
import redelm.data.Group;
import redelm.data.GroupRecordConsumer;
import redelm.data.GroupWriter;
import redelm.data.simple.SimpleGroup;
import redelm.data.simple.SimpleGroupFactory;
import redelm.io.RecordConsumerWrapper;
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
  public void test() throws ExecException, ParserException {
    List<Tuple> tuples = new ArrayList<Tuple>();

    TupleRecordConsumer tupleRecordConsumer = new TupleRecordConsumer(schema,
        Utils.getSchemaFromString(
        "DocId:long, " +
        "Links:(Backward:{(long)}, Forward:{(long)}), " +
        "Name:{(Language:{(Code:chararray,Country:chararray)}, Url:chararray)}"),
        tuples);
    GroupWriter groupWriter = new GroupWriter(new RecordConsumerWrapper(tupleRecordConsumer), schema);
    groupWriter.write(r1);
    groupWriter.write(r2);

    for (Tuple t : tuples) {
      logger.debug(t);
    }

    List<Group> groups = new ArrayList<Group>();
    TupleWriter tupleWriter = new TupleWriter(new RecordConsumerWrapper(new GroupRecordConsumer(new SimpleGroupFactory(schema), groups)), schema);
    for (Tuple t : tuples) {
      logger.debug(t);
      tupleWriter.write(t);
    }
    assertEquals(r1.toString(), groups.get(0).toString());
    assertEquals(r2.toString(), groups.get(1).toString());
  }

  @Test
  public void testMaps() throws ExecException, ParserException {
    List<Tuple> tuples = new ArrayList<Tuple>();

    String pigSchemaString = "a: [(b: chararray)]";
    Schema pigSchema = Utils.getSchemaFromString(pigSchemaString);
    MessageType redelmSchema = new PigSchemaConverter().convert(pigSchema);
    System.out.println(redelmSchema);
    TupleRecordConsumer tupleRecordConsumer = new TupleRecordConsumer(redelmSchema, pigSchema, tuples);
    GroupWriter groupWriter = new GroupWriter(new RecordConsumerWrapper(tupleRecordConsumer), redelmSchema);
    SimpleGroup g = new SimpleGroup(redelmSchema);
    g.addGroup("a").append("key", "foo").addGroup("a").append("b", "foo");
    g.addGroup("a").append("key", "bar").addGroup("a").append("b", "bar");
    groupWriter.write(g);

    for (Tuple t : tuples) {
      logger.debug(t);
    }

    List<Group> groups = new ArrayList<Group>();
    TupleWriter tupleWriter = new TupleWriter(new RecordConsumerWrapper(new GroupRecordConsumer(new SimpleGroupFactory(redelmSchema), groups)), redelmSchema);
    for (Tuple t : tuples) {
      logger.debug(t);
      tupleWriter.write(t);
    }
    Assert.assertEquals(g.toString(), groups.get(0).toString());
  }

}
