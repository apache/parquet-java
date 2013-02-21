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
package parquet.pig.summary;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

import parquet.pig.summary.MapSummaryData;
import parquet.pig.summary.Summary;
import parquet.pig.summary.SummaryData;
import parquet.pig.summary.TupleSummaryData;

public class TestSummary {

  private static final TupleFactory tf = TupleFactory.getInstance();
  private static final BagFactory bf = BagFactory.getInstance();

  private static final DataBag TEST_BAG = b(
      t(b(t(1l), t(2l, m("foo", "bar")), t(3))),
      t(b(t(1l), t(1l), t(3, "blah"))),
      t(b(t(1l), t(2l), t(2, "bloh"))),
      t(b(t(1l), null, t(2, "bloh"))),
      t(b(t("foo"), null, t(2, "bloh"))),
      t(b(t(b(t("bar"))), null, t(2, "bloh"))),
      t(b(t(b(t("bar"))), t(1l, m("foo", "bar", "baz", "buz")), t(2, "bloh"))),
      t(),
      t(null, null)
      );

  public static Tuple t(Object... objects) {
    return tf.newTuple(Arrays.asList(objects));
  }

  public static DataBag b(Tuple... tuples) {
    return bf.newDefaultBag(Arrays.asList(tuples));
  }

  public static Map<String, Object> m(Object... objects) {
    Map<String, Object> m = new HashMap<String, Object>();
    for (int i = 0; i < objects.length; i += 2) {
      m.put((String)objects[i], objects[i + 1]);
    }
    return m;
  }

  @Test
  public void testEvalFunc() throws IOException {
    Summary summary = new Summary();
    String result = summary.exec(t(TEST_BAG));
    validate(result, 1);
  }


  @Test
  public void testAlgebraic() throws IOException {
    Summary.Initial initial = new Summary.Initial();
    Summary.Intermediate intermediate1 = new Summary.Intermediate();
    Summary.Intermediate intermediate2 = new Summary.Intermediate();
    Summary.Final finall = new Summary.Final();

    DataBag combinedRedIn = bf.newDefaultBag();
    for (int r = 0; r < 5; r++) {
      DataBag combinedMapOut = bf.newDefaultBag();
      for (int m = 0; m < 5; m++) {
        DataBag mapOut = bf.newDefaultBag();
        for (Tuple t : TEST_BAG) {
          Tuple exec = initial.exec(t(b(t)));
          mapOut.add(exec);
        }
        Tuple exec = intermediate1.exec(t(mapOut));
        validate((String)exec.get(0), 1);
        combinedMapOut.add(exec);
      }
      combinedRedIn.add(intermediate2.exec(t(combinedMapOut)));
    }
    String result = finall.exec(t(combinedRedIn));
    validate(result, 5*5);

  }

  private void validate(String result, int factor) throws JsonParseException, JsonMappingException, IOException {
    TupleSummaryData s = SummaryData.fromJSON(result, TupleSummaryData.class);
//          System.out.println(SummaryData.toPrettyJSON(s));
    assertEquals(9 * factor, s.getCount());
    assertEquals(1 * factor, s.getFields().get(0).getNull().longValue());
    assertEquals(7 * factor, s.getFields().get(0).getBag().getCount());
    assertEquals(15 * factor,
        s.getFields().get(0).getBag().getContent().getTuple().getFields().get(0).getCount());
    MapSummaryData map =
        s.getFields().get(0).getBag().getContent().getTuple().getFields().get(1).getMap();
    assertEquals(2 * factor, map.getCount());
    assertEquals(3 * factor, map.getKey().getCount());
  }

  @Test
  public void testPigScript() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Data data = Storage.resetData(pigServer);
    List<Tuple> list = new ArrayList<Tuple>();
    for (int i = 0; i < 1002; i++) {
      list.add(t("a", "b" + i, 1l, b(t("a", m("foo", "bar")))));
    }
    data.set("in", "a:chararray, a1:chararray, b:int, c:{t:(a2:chararray, b2:[])}", list);
    pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
    pigServer.registerQuery("B = FOREACH (GROUP A ALL) GENERATE "+Summary.class.getName()+"(A);");
    pigServer.registerQuery("STORE B INTO 'out' USING mock.Storage();");
    System.out.println(data.get("out").get(0).get(0));
    TupleSummaryData s = SummaryData.fromJSON((String)data.get("out").get(0).get(0), TupleSummaryData.class);
    System.out.println(s);
  }


}
