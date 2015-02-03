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
package parquet.thrift.pig;

import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import parquet.pig.ParquetLoader;
import parquet.thrift.test.Name;

public class TestParquetThriftStorer {
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
      list.add(tuple("bob", "roberts" + i));
    }
    data.set("in", "fn:chararray, ln:chararray", list );
    pigServer.deleteFile(out);
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.registerQuery("Store A into '"+out+"' using "+ParquetThriftStorer.class.getName()+"('" + Name.class.getName() + "');");
    execBatch(pigServer);

    pigServer.registerQuery("B = LOAD '"+out+"' USING "+ParquetLoader.class.getName()+"();");
    pigServer.registerQuery("Store B into 'out' using mock.Storage();");
    execBatch(pigServer);

    List<Tuple> result = data.get("out");

    assertEquals(rows, result.size());
    int i = 0;
    for (Tuple tuple : result) {
      assertEquals(tuple("bob", "roberts" + i), tuple);
      ++i;
    }
  }

  private void execBatch(PigServer pigServer) throws IOException {
    if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
      throw new RuntimeException("Job failed", pigServer.executeBatch().get(0).getException());
    }
  }
}
