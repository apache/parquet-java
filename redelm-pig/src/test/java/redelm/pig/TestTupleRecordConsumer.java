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
import redelm.data.simple.SimpleGroupFactory;
import redelm.io.RecordConsumerWrapper;

public class TestTupleRecordConsumer {
  private static final Log logger = Log.getLog(TestTupleRecordConsumer.class);

  @Test
  public void test() throws ExecException {
    List<Tuple> tuples = new ArrayList<Tuple>();
    TupleRecordConsumer tupleRecordConsumer = new TupleRecordConsumer(schema, tuples);
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

}
