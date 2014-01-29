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
package parquet.pig;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestParquetLoader {
  @Test
  public void testSchema() throws Exception {
    String location = "garbage";
    ParquetLoader pLoader = new ParquetLoader(
        "a:chararray, " +
        "b:{t:(c:chararray, d:chararray)}, " +
        "p:[(q:chararray, r:chararray)]");
    Job job = new Job();
    pLoader.getSchema(location, job);
    RequiredFieldList list = new RequiredFieldList();
    RequiredField field = new RequiredField("a", 0, null, DataType.CHARARRAY);
    list.add(field);
    field = new RequiredField("b", 0,
        Arrays.asList(new RequiredField("t", 0,
            Arrays.asList(new RequiredField("d", 1, null, DataType.CHARARRAY)),
                DataType.TUPLE)),
                DataType.BAG);
    list.add(field);
    pLoader.pushProjection(list);
    pLoader.setLocation(location, job);

    assertEquals("{a: chararray,b: {t: (d: chararray)}}",
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
      list.add(Storage.tuple(i, "a"+i, i*2));
    }
    data.set("in", "i:int, a:chararray, b:int", list );
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"' using " + ParquetStorer.class.getName()+"();");
    pigServer.executeBatch();

    List<Tuple> expectedList = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      expectedList.add(Storage.tuple("a" + i));
    }
    
    pigServer.registerQuery("C = LOAD '" + out + "' using " + ParquetLoader.class.getName()+"();");
    pigServer.registerQuery("D = foreach C generate a;");
    pigServer.registerQuery("Store D into 'out' using mock.Storage();");
    pigServer.executeBatch();
    List<Tuple> actualList = data.get("out");
    
    pigServer.registerQuery("C = LOAD '" + out + "' using " + ParquetLoader.class.getName()+"('a:chararray, b:int');");
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
    pigServer.registerQuery("C = LOAD '" + out + "' using " + ParquetLoader.class.getName()+"('a:chararray, b:int');");
    pigServer.registerQuery("D = foreach C generate a;");
    pigServer.registerQuery("Store D into 'out' using mock.Storage();");
    pigServer.executeBatch();
    actualList = data.get("out");
    Assert.assertEquals(expectedList, actualList);
  }
  
  @Test
  public void testRead() {
    
  }
}
