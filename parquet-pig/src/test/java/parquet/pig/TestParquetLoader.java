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
        TupleReadSupport.getRequestedPigSchema(job.getConfiguration()).toString());
  }
}
