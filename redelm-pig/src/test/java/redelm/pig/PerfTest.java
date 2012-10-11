package redelm.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

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
      pigServer.registerQuery("Store A into '"+out+"' using "+RedelmStorer.class.getName()+"();");

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
    pigServer.registerQuery("B = LOAD '"+out+"' USING "+RedelmLoader.class.getName()+"('"+schemaString+"');");
    pigServer.registerQuery("C = FOREACH (GROUP B ALL) GENERATE COUNT(B);");
    Iterator<Tuple> it = pigServer.openIterator("C");
    if (!it.hasNext()) {
      throw new RuntimeException("Job failed: no tuple to read");
    }
    Long count = (Long)it.next().get(0);

    Assert.assertEquals(ROW_COUNT, count.longValue());
    long t1 = System.currentTimeMillis();
    results.append((t1-t0)+" ms to read "+colsToLoad+" columns\n");
  }

}
