package parquet.pig;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.junit.Ignore;
import org.junit.Test;

public class TestParquetLoader {
  @Test
  public void testProjectionPushdown() throws Exception {
    String out = "target/out";
    int rows = 1000;
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Data data = Storage.resetData(pigServer);
    Collection<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < rows; i++) {
      list.add(Storage.tuple(i, "a"+i));
    }
    data.set("in", "i:int, a:chararray", list );
    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.deleteFile(out);
    pigServer.registerQuery("Store A into '"+out+"' using "+ParquetStorer.class.getName()+"();");
    
    long bytesWritten = pigServer.executeBatch().get(0).getStatistics().getBytesWritten();
    
    pigServer = new PigServer(ExecType.LOCAL);
    pigServer.setBatchOn();
    data = Storage.resetData(pigServer);
    pigServer.registerQuery("A = LOAD '" + out + "' using " + ParquetLoader.class.getName()+"();");
    pigServer.registerQuery("B = foreach A generate i;");
    pigServer.registerQuery("Store B into 'foo' using mock.Storage();");
    
    long bytesRead = pigServer.executeBatch().get(0).getStatistics().getBytesWritten();
    
    System.out.println(bytesWritten);
    System.out.println(bytesRead);
    
    
    
    
  }
}
