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
package org.apache.parquet.pig;

import static org.apache.parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;
import static org.apache.pig.data.DataType.BOOLEAN;
import static org.apache.pig.data.DataType.CHARARRAY;
import static org.apache.pig.data.DataType.DOUBLE;
import static org.apache.pig.data.DataType.FLOAT;
import static org.apache.pig.data.DataType.INTEGER;
import static org.apache.pig.data.DataType.LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestParquetLoader {

  private static final Logger LOG = LoggerFactory.getLogger(TestParquetLoader.class);

  @Test
  public void testSchema() throws Exception {
    String location = "garbage";
    ParquetLoader pLoader = new ParquetLoader(
        "a:chararray, " + "b:{t:(c:chararray, d:chararray)}, " + "p:[(q:chararray, r:chararray)]");
    Job job = new Job();
    pLoader.getSchema(location, job);
    RequiredFieldList list = new RequiredFieldList();
    RequiredField field = new RequiredField("a", 0, null, DataType.CHARARRAY);
    list.add(field);
    field = new RequiredField(
        "b",
        0,
        Arrays.asList(new RequiredField(
            "t", 0, Arrays.asList(new RequiredField("d", 1, null, DataType.CHARARRAY)), DataType.TUPLE)),
        DataType.BAG);
    list.add(field);
    pLoader.pushProjection(list);
    pLoader.setLocation(location, job);

    assertEquals(
        "{a: chararray,b: {t: (d: chararray)}}",
        TupleReadSupport.getPigSchema(job.getConfiguration()).toString());
  }

  @Test
  public void testProjectionPushdown() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    pigServer.setValidateEachStatement(true);
    String out = "target/out";
    int rows = 10;
    Data data = Storage.resetData(pigServer);
    List<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, "a" + i, i * 2));
    }
    data.set("in", "i:int, a:chararray, b:int", list);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '" + out + "' using " + ParquetStorer.class.getName() + "();");
    pigServer.executeBatch();

    List<Tuple> expectedList = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      expectedList.add(Storage.tuple("a" + i));
    }

    pigServer.registerQuery("C = LOAD '" + out + "' using " + ParquetLoader.class.getName() + "();");
    pigServer.registerQuery("D = foreach C generate a;");
    pigServer.registerQuery("Store D into 'out' using mock.Storage();");
    pigServer.executeBatch();
    List<Tuple> actualList = data.get("out");

    pigServer.registerQuery(
        "C = LOAD '" + out + "' using " + ParquetLoader.class.getName() + "('a:chararray, b:int');");
    Assert.assertEquals("{a: chararray,b: int}", pigServer.dumpSchema("C").toString());
    try {
      pigServer.registerQuery("D = foreach C generate i;");
      Assert.fail("Frontend Exception expected");
    } catch (FrontendException fe) {
    }

    // we need to reset pigserver here as there is some problem is pig
    // that causes pigserver to behave mysteriously after the exception
    // TODO investigate if its is fixed in pig trunk
    pigServer = new PigServer(ExecType.LOCAL);
    data = Storage.resetData(pigServer);
    pigServer.setBatchOn();
    pigServer.registerQuery(
        "C = LOAD '" + out + "' using " + ParquetLoader.class.getName() + "('a:chararray, b:int');");
    pigServer.registerQuery("D = foreach C generate a;");
    pigServer.registerQuery("Store D into 'out' using mock.Storage();");
    pigServer.executeBatch();
    actualList = data.get("out");
    Assert.assertEquals(expectedList, actualList);
  }

  @Test
  public void testNullPadding() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    pigServer.setValidateEachStatement(true);
    String out = "target/out";
    int rows = 10;
    Data data = Storage.resetData(pigServer);
    List<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, "a" + i, i * 2));
    }
    data.set("in", "i:int, a:chararray, b:int", list);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '" + out + "' using " + ParquetStorer.class.getName() + "();");
    pigServer.executeBatch();

    // Test Null Padding at the end
    pigServer.registerQuery("C = LOAD '" + out + "' using " + ParquetLoader.class.getName()
        + "('i:int, a:chararray, b:int, n1:int, n2:chararray');");
    pigServer.registerQuery("STORE C into 'out' using mock.Storage();");
    pigServer.executeBatch();

    List<Tuple> actualList = data.get("out");

    assertEquals(rows, actualList.size());
    for (Tuple t : actualList) {
      assertTrue(t.isNull(3));
      assertTrue(t.isNull(4));
    }

    // Test Null padding mixed in with sub-select
    pigServer.registerQuery("D = LOAD '" + out + "' using " + ParquetLoader.class.getName()
        + "('n1:int, a:chararray, n2:chararray, b:int');");
    pigServer.registerQuery("STORE D into 'out2' using mock.Storage();");
    pigServer.executeBatch();

    actualList = data.get("out2");

    assertEquals(rows, actualList.size());
    for (Tuple t : actualList) {
      assertTrue(t.isNull(0));
      assertTrue(t.isNull(2));
    }
  }

  @Test
  public void testReqestedSchemaColumnPruning() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    pigServer.setValidateEachStatement(true);
    String out = "target/out";
    int rows = 10;
    Data data = Storage.resetData(pigServer);
    List<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, "a" + i, i * 2));
    }
    data.set("in", "i:int, a:chararray, b:int", list);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '" + out + "' using " + ParquetStorer.class.getName() + "();");
    pigServer.executeBatch();

    // Test Null Padding at the end
    pigServer.registerQuery("C = LOAD '" + out + "' using " + ParquetLoader.class.getName()
        + "('i:int, a:chararray, b:int, n1:int, n2:chararray');");
    pigServer.registerQuery("G = foreach C generate n1,b,n2,i;");
    pigServer.registerQuery("STORE G into 'out' using mock.Storage();");
    pigServer.executeBatch();

    List<Tuple> actualList = data.get("out");

    assertEquals(rows, actualList.size());
    for (Tuple t : actualList) {
      assertEquals(4, t.size());
      assertTrue(t.isNull(0));
      assertTrue(t.isNull(2));
    }
  }

  @Test
  public void testTypePersuasion() throws Exception {
    Properties p = new Properties();
    p.setProperty(STRICT_TYPE_CHECKING, Boolean.FALSE.toString());

    PigServer pigServer = new PigServer(ExecType.LOCAL, p);
    pigServer.setValidateEachStatement(true);
    String out = "target/out";
    int rows = 10;
    Data data = Storage.resetData(pigServer);
    List<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, (long) i, (float) i, (double) i, Integer.toString(i), Boolean.TRUE));
    }
    data.set("in", "i:int, l:long, f:float, d:double, s:chararray, b:boolean", list);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '" + out + "' using " + ParquetStorer.class.getName() + "();");
    pigServer.executeBatch();

    List<Tuple> actualList = null;

    byte[] types = {INTEGER, LONG, FLOAT, DOUBLE, CHARARRAY, BOOLEAN};

    // Test extracting values using each type.
    for (int i = 0; i < types.length; i++) {
      String query = "B = LOAD '" + out + "' using " + ParquetLoader.class.getName() + "('i:"
          + DataType.findTypeName(types[i % types.length]) + "," + "  l:"
          + DataType.findTypeName(types[(i + 1) % types.length]) + "," + "  f:"
          + DataType.findTypeName(types[(i + 2) % types.length]) + "," + "  d:"
          + DataType.findTypeName(types[(i + 3) % types.length]) + "," + "  s:"
          + DataType.findTypeName(types[(i + 4) % types.length]) + "," + "  b:"
          + DataType.findTypeName(types[(i + 5) % types.length]) + "');";

      LOG.info("Query: " + query);
      pigServer.registerQuery(query);
      pigServer.registerQuery("STORE B into 'out" + i + "' using mock.Storage();");
      pigServer.executeBatch();

      actualList = data.get("out" + i);

      assertEquals(rows, actualList.size());
      for (Tuple t : actualList) {
        assertTrue(t.getType(0) == types[i % types.length]);
        assertTrue(t.getType(1) == types[(i + 1) % types.length]);
        assertTrue(t.getType(2) == types[(i + 2) % types.length]);
        assertTrue(t.getType(3) == types[(i + 3) % types.length]);
        assertTrue(t.getType(4) == types[(i + 4) % types.length]);
        assertTrue(t.getType(5) == types[(i + 5) % types.length]);
      }
    }
  }

  @Test
  public void testColumnIndexAccess() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    pigServer.setValidateEachStatement(true);
    String out = "target/out";
    int rows = 10;
    Data data = Storage.resetData(pigServer);
    List<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, i * 1.0, i * 2L, "v" + i));
    }
    data.set("in", "c1:int, c2:double, c3:long, c4:chararray", list);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '" + out + "' using " + ParquetStorer.class.getName() + "();");
    pigServer.executeBatch();

    // Test Null Padding at the end
    pigServer.registerQuery("B = LOAD '" + out + "' using " + ParquetLoader.class.getName()
        + "('n1:int, n2:double, n3:long, n4:chararray', 'true');");
    pigServer.registerQuery("STORE B into 'out' using mock.Storage();");
    pigServer.executeBatch();

    List<Tuple> actualList = data.get("out");

    assertEquals(rows, actualList.size());
    for (int i = 0; i < rows; i++) {
      Tuple t = actualList.get(i);

      assertEquals(4, t.size());

      assertEquals(i, t.get(0));
      assertEquals(i * 1.0, t.get(1));
      assertEquals(i * 2L, t.get(2));
      assertEquals("v" + i, t.get(3));
    }
  }

  @Test
  public void testColumnIndexAccessProjection() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    pigServer.setValidateEachStatement(true);
    String out = "target/out";
    int rows = 10;
    Data data = Storage.resetData(pigServer);
    List<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, i * 1.0, i * 2L, "v" + i));
    }
    data.set("in", "c1:int, c2:double, c3:long, c4:chararray", list);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '" + out + "' using " + ParquetStorer.class.getName() + "();");
    pigServer.executeBatch();

    pigServer.registerQuery("B = LOAD '" + out + "' using " + ParquetLoader.class.getName()
        + "('n1:int, n2:double, n3:long, n4:chararray', 'true');");
    pigServer.registerQuery("C = foreach B generate n1, n3;");
    pigServer.registerQuery("STORE C into 'out' using mock.Storage();");
    pigServer.executeBatch();

    List<Tuple> actualList = data.get("out");

    assertEquals(rows, actualList.size());
    for (int i = 0; i < rows; i++) {
      Tuple t = actualList.get(i);

      assertEquals(2, t.size());

      assertEquals(i, t.get(0));
      assertEquals(i * 2L, t.get(1));
    }
  }

  @Test
  public void testPredicatePushdown() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetLoader.ENABLE_PREDICATE_FILTER_PUSHDOWN, true);

    PigServer pigServer = new PigServer(ExecType.LOCAL, conf);
    pigServer.setValidateEachStatement(true);

    String out = "target/out";
    String out2 = "target/out2";
    int rows = 10;
    Data data = Storage.resetData(pigServer);
    List<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, i * 1.0, i * 2L, "v" + i));
    }
    data.set("in", "c1:int, c2:double, c3:long, c4:chararray", list);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '" + out + "' using " + ParquetStorer.class.getName() + "();");
    pigServer.executeBatch();

    pigServer.deleteFile(out2);
    pigServer.registerQuery("B = LOAD '" + out + "' using " + ParquetLoader.class.getName()
        + "('c1:int, c2:double, c3:long, c4:chararray');");
    pigServer.registerQuery("C = FILTER B by c1 == 1 or c1 == 5;");
    pigServer.registerQuery("STORE C into '" + out2 + "' using mock.Storage();");
    List<ExecJob> jobs = pigServer.executeBatch();

    long recordsRead = jobs.get(0).getStatistics().getInputStats().get(0).getNumberRecords();

    assertEquals(2, recordsRead);
  }
}
