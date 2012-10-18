package redelm.pig;

import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

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

    assertEquals(1000, result.size());
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

      assertEquals(list, result);
    }

    {
      pigServer.registerQuery("C = LOAD '"+out+"' USING "+RedelmLoader.class.getName()+"('a:chararray');");
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
