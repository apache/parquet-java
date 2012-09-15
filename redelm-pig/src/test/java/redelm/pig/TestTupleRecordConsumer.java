package redelm.pig;

import static redelm.data.simple.example.Paper.r1;
import static redelm.data.simple.example.Paper.r2;
import static redelm.data.simple.example.Paper.schema;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import redelm.Log;
import redelm.data.Group;
import redelm.data.GroupRecordConsumer;
import redelm.data.GroupWriter;
import redelm.data.simple.SimpleGroupFactory;
import redelm.io.RecordConsumerWrapper;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;

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
    Assert.assertEquals(r1.toString(), groups.get(0).toString());
    Assert.assertEquals(r2.toString(), groups.get(1).toString());
  }

}
