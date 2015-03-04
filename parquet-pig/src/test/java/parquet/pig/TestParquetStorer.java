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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Test;

import parquet.pig.ParquetLoader;
import parquet.pig.ParquetStorer;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

public class TestParquetStorer {

  @Test
  public void testStorer() throws ExecException, Exception {
    String out = "target/out";
    int rows = 1000;
    Properties props = new Properties();
    props.setProperty("parquet.compression", "uncompressed");
    props.setProperty("parquet.page.size", "1000");
    PigServer pigServer = new PigServer(ExecType.LOCAL, props);
    Data data = Storage.resetData(pigServer);
    Collection<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(tuple("a"+i));
    }
    data.set("in", "a:chararray", list );
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"' using "+ParquetStorer.class.getName()+"();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    pigServer.registerQuery("B = LOAD '"+out+"' USING "+ParquetLoader.class.getName()+"();");
    pigServer.registerQuery("Store B into 'out' using mock.Storage();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    List<Tuple> result = data.get("out");

    assertEquals(rows, result.size());
    int i = 0;
    for (Tuple tuple : result) {
      assertEquals("a"+i, tuple.get(0));
      ++i;
    }
  }

  @Test
  public void testMultipleSchema() throws ExecException, Exception {
    String out = "target/out";
    int rows = 1000;
    Properties props = new Properties();
    props.setProperty("parquet.compression", "uncompressed");
    props.setProperty("parquet.page.size", "1000");
    PigServer pigServer = new PigServer(ExecType.LOCAL, props);
    Data data = Storage.resetData(pigServer);
    Collection<Tuple> list1 = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list1.add(tuple("a"+i));
    }
    Collection<Tuple> list2 = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list2.add(tuple("b"+i));
    }
    data.set("a", "a:chararray", list1);
    data.set("b", "b:chararray", list2);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'a' USING mock.Storage();");
    pigServer.registerQuery("B = LOAD 'b' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"/a' using "+ParquetStorer.class.getName()+"();");
    pigServer.registerQuery("Store B into '"+out+"/b' using "+ParquetStorer.class.getName()+"();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    pigServer.registerQuery("B = LOAD '"+out+"/*' USING "+ParquetLoader.class.getName()+"();");
    pigServer.registerQuery("Store B into 'out' using mock.Storage();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    List<Tuple> result = data.get("out");

    final Schema schema = data.getSchema("out");
    assertEquals(2, schema.size());
    // union could be in either order
    int ai;
    int bi;
    if ("a".equals(schema.getField(0).alias)) {
      ai = 0;
      bi = 1;
      assertEquals("a", schema.getField(0).alias);
      assertEquals("b", schema.getField(1).alias);
    } else {
      ai = 1;
      bi = 0;
      assertEquals("b", schema.getField(0).alias);
      assertEquals("a", schema.getField(1).alias);
    }

    assertEquals(rows * 2, result.size());

    int a = 0;
    int b = 0;
    for (Tuple tuple : result) {
      String fa = (String) tuple.get(ai);
      String fb = (String) tuple.get(bi);
      if (fa != null) {
        assertEquals("a" + a, fa);
        ++a;
      }
      if (fb != null) {
        assertEquals("b" + b, fb);
        ++b;
      }
    }

  }

  @Test
  public void testStorerCompressed() throws ExecException, Exception {
    String out = "target/out";
    int rows = 1000;
    Properties props = new Properties();
    props.setProperty("parquet.compression", "gzip");
    props.setProperty("parquet.page.size", "1000");
    PigServer pigServer = new PigServer(ExecType.LOCAL, props);
    Data data = Storage.resetData(pigServer);
    Collection<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple("a"+i));
    }
    data.set("in", "a:chararray", list );
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"' using "+ParquetStorer.class.getName()+"();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    pigServer.registerQuery("B = LOAD '"+out+"' USING "+ParquetLoader.class.getName()+"();");
    pigServer.registerQuery("Store B into 'out' using mock.Storage();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    List<Tuple> result = data.get("out");

    assertEquals(rows, result.size());
    int i = 0;
    for (Tuple tuple : result) {
      assertEquals("a"+i, tuple.get(0));
      ++i;
    }

  }

  @Test
  public void testComplexSchema() throws ExecException, Exception {
    String out = "target/out";
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Data data = Storage.resetData(pigServer);
    Collection<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < 1000; i++) {
      list.add(tuple("a"+i, bag(tuple("o", "b"))));
    }
    for (int i = 10; i < 2000; i++) {
      list.add(tuple("a"+i, bag(tuple("o", "b"), tuple("o", "b"), tuple("o", "b"), tuple("o", "b"))));
    }
    for (int i = 20; i < 3000; i++) {
      list.add(tuple("a"+i, bag(tuple("o", "b"), tuple("o", null), tuple(null, "b"), tuple(null, null))));
    }
    for (int i = 30; i < 4000; i++) {
      list.add(tuple("a"+i, null));
    }
    Collections.shuffle((List<?>)list);
    data.set("in", "a:chararray, b:{t:(c:chararray, d:chararray)}", list );
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"' using "+ParquetStorer.class.getName()+"();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    {
      pigServer.registerQuery("B = LOAD '"+out+"' USING "+ParquetLoader.class.getName()+"();");
      pigServer.registerQuery("Store B into 'out' using mock.Storage();");
      if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
        throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
      }

      List<Tuple> result = data.get("out");
      assertEquals(list, result);
      final Schema schema = data.getSchema("out");
      assertEquals("{a:chararray, b:{t:(c:chararray, d:chararray)}}".replaceAll(" ", ""), schema.toString().replaceAll(" ", ""));
    }

    {
      pigServer.registerQuery("C = LOAD '"+out+"' USING "+ParquetLoader.class.getName()+"('a:chararray');");
      pigServer.registerQuery("Store C into 'out2' using mock.Storage();");
      if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
        throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
      }

      final Function<Tuple,Object> grabFirstColumn = new Function<Tuple,Object>() {
        @Override
        public Object apply(Tuple input) {
          try {
              return input.get(0);
          } catch (ExecException e) {
            throw new RuntimeException(e);
          }
        }
      };

      List<Tuple> result2 = data.get("out2");
      // Functional programming!!
      Object[] result2int = Collections2.transform(result2, grabFirstColumn).toArray();
      Object[] input2int = Collections2.transform(list, grabFirstColumn).toArray();

      assertArrayEquals(input2int, result2int);
    }
  }
}
