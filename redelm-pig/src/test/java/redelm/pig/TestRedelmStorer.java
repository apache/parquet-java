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

import static org.apache.pig.builtin.mock.Storage.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestRedelmStorer {

  @Test
  public void testStorer() throws ExecException, Exception {
    String out = "target/out";
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Data data = Storage.resetData(pigServer);
    Collection<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < 1000; i++) {
      list.add(Storage.tuple("a"+i));
    }
    data.set("in", "a:chararray", list );
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"' using "+RedelmStorer.class.getName()+"();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    pigServer.registerQuery("B = LOAD '"+out+"' USING "+RedelmLoader.class.getName()+"();");
    pigServer.registerQuery("Store B into 'out' using mock.Storage();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    List<Tuple> result = data.get("out");

    Assert.assertEquals(1000, result.size());
    int i = 0;
    for (Tuple tuple : result) {
      Assert.assertEquals("a"+i, tuple.get(0));
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
    data.set("in", "a:chararray, b:{t:(c:chararray, d:chararray)}", list );
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"' using "+RedelmStorer.class.getName()+"();");
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }

    {
      pigServer.registerQuery("B = LOAD '"+out+"' USING "+RedelmLoader.class.getName()+"();");
      pigServer.registerQuery("Store B into 'out' using mock.Storage();");
      if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
        throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
      }

      List<Tuple> result = data.get("out");

      Assert.assertEquals(1000, result.size());
      int i = 0;
      for (Tuple tuple : result) {
        Assert.assertEquals("a"+i, tuple.get(0));
        Assert.assertEquals("{(o,b)}", tuple.get(1).toString());
        ++i;
      }
    }

    {
      pigServer.registerQuery("C = LOAD '"+out+"' USING "+RedelmLoader.class.getName()+"('a:chararray');");
      pigServer.registerQuery("Store C into 'out2' using mock.Storage();");
      if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
        throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
      }

      List<Tuple> result2 = data.get("out2");

      Assert.assertEquals(1000, result2.size());
      int i = 0;
      for (Tuple tuple : result2) {
        Assert.assertEquals(1, tuple.size());
        Assert.assertEquals("a"+i, tuple.get(0));
        ++i;
      }
    }
  }
}
