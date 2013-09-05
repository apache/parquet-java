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

import java.util.Arrays;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.data.DataType;
import org.junit.Test;

public class TestParquetLoader {
  @Test
  public void testSchema() throws Exception {
    String location = "target/out";
    ParquetLoader pLoader = new ParquetLoader("a:chararray, b:{t:(c:chararray, d:chararray)}, p:[(q:chararray, r:chararray)]");
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
}
