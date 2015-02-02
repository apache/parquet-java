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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import parquet.pig.ParquetLoader;
import parquet.pig.ParquetStorer;

/**
 *
 * some hardcoded latencies in hadoop prevent any information to come out of this test
 *
 * @author Julien Le Dem
 *
 */
public class PerfTest {
  private static final int COLUMN_COUNT = 50;
  private static final long ROW_COUNT = 100000;
  private static StringBuilder results = new StringBuilder();

  public static void main(String[] args) throws Exception {

    StringBuilder schemaString = new StringBuilder("a0: chararray");
    for (int i = 1; i < COLUMN_COUNT; i++) {
      schemaString.append(", a" + i + ": chararray");
    }
    String out = "target/PerfTest";
    {
      PigServer pigServer = new PigServer(ExecType.LOCAL);
      Data data = Storage.resetData(pigServer);
      Collection<Tuple> list = new ArrayList<Tuple>();
      for (int i = 0; i < ROW_COUNT; i++) {
        Tuple tuple = TupleFactory.getInstance().newTuple(COLUMN_COUNT);
        for (int j = 0; j < COLUMN_COUNT; j++) {
          tuple.set(j, "a" + i + "_" + j);
        }
        list.add(tuple);
      }
      data.set("in", schemaString.toString(), list);
      pigServer.setBatchOn();
      pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
      pigServer.deleteFile(out);
      pigServer.registerQuery("Store A into '"+out+"' using "+ParquetStorer.class.getName()+"();");

      if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
        throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
      }
    }
    load(out, 1);
    load(out, 2);
    load(out, 3);
    load(out, 4);
    load(out, 5);
    load(out, 10);
    load(out, 20);
    load(out, 50);
    System.out.println(results);
  }

  private static void load(String out, int colsToLoad) throws ExecException, IOException {
    long t0 = System.currentTimeMillis();
    StringBuilder schemaString = new StringBuilder("a0: chararray");
    for (int i = 1; i < colsToLoad; i++) {
      schemaString.append(", a" + i + ": chararray");
    }
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    pigServer.registerQuery("B = LOAD '"+out+"' USING "+ParquetLoader.class.getName()+"('"+schemaString+"');");
    pigServer.registerQuery("C = FOREACH (GROUP B ALL) GENERATE COUNT(B);");
    Iterator<Tuple> it = pigServer.openIterator("C");
    if (!it.hasNext()) {
      throw new RuntimeException("Job failed: no tuple to read");
    }
    Long count = (Long)it.next().get(0);

    assertEquals(ROW_COUNT, count.longValue());
    long t1 = System.currentTimeMillis();
    results.append((t1-t0)+" ms to read "+colsToLoad+" columns\n");
  }

}
