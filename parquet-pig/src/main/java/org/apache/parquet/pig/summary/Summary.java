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
package org.apache.parquet.pig.summary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * computes a summary of the input to a json string
 */
public class Summary extends EvalFunc<String> implements Algebraic {

  private static final TupleFactory TF = TupleFactory.getInstance();

  public static class Initial extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple t) throws IOException {
      return new JSONTuple(sumUp(getInputSchema(), t));
    }
  }

  public static class Intermediate extends EvalFunc<Tuple> {
    @Override
    public Tuple exec(Tuple t) throws IOException {
      return new JSONTuple(merge(t));
    }
  }

  public static class Final extends EvalFunc<String> {
    @Override
    public String exec(Tuple t) throws IOException {
      return SummaryData.toPrettyJSON(merge(t));
    }
  }

  private static final class JSONTuple implements Tuple {
    private static final long serialVersionUID = 1L;
    private TupleSummaryData data;

    public JSONTuple(TupleSummaryData data) {
      this.data = data;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      Tuple t = TF.newTuple(json());
      t.write(dataOutput);
    }

    @Override
    public int compareTo(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void append(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int i) throws ExecException {
      if (i == 0) {
        return json();
      }
      throw new ExecException();
    }

    private String json() {
      return SummaryData.toJSON(data);
    }

    @Override
    public List<Object> getAll() {
      return new ArrayList<Object>(Arrays.asList(json()));
    }

    @Override
    public long getMemorySize() {
      // I don't know. Not too big and we're not going to have many
      return 100;
    }

    @Override
    public byte getType(int i) throws ExecException {
      if (i == 0) {
        return DataType.CHARARRAY;
      }
      throw new ExecException("size is 1");
    }

    @Override
    public boolean isNull(int i) throws ExecException {
      if (i == 0) {
        return false;
      }
      throw new ExecException("size is 1");
    }

    @Override
    public void reference(Tuple t) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void set(int i, Object o) throws ExecException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    public String toDelimitedString(String delim) throws ExecException {
      return json();
    }

    @Override
    public Iterator<Object> iterator() {
      return getAll().iterator();
    }
  }

  private static TupleSummaryData getData(Tuple tuple) throws ExecException {
    if (tuple instanceof JSONTuple) {
      return ((JSONTuple) tuple).data;
    } else {
      return SummaryData.fromJSON((String) tuple.get(0), TupleSummaryData.class);
    }
  }

  /**
   * the input tuple contains a bag of string representations of TupleSummaryData
   *
   * @param t
   * @return
   * @throws ExecException
   * @throws IOException
   */
  private static TupleSummaryData merge(Tuple t) throws IOException {
    TupleSummaryData summaryData = new TupleSummaryData();
    DataBag bag = (DataBag) t.get(0);
    for (Tuple tuple : bag) {
      summaryData.merge(getData(tuple));
    }
    return summaryData;
  }

  /**
   * The input tuple contains a bag of Tuples to sum up
   *
   * @param t
   * @return
   * @throws ExecException
   */
  private static TupleSummaryData sumUp(Schema schema, Tuple t) throws ExecException {
    TupleSummaryData summaryData = new TupleSummaryData();
    DataBag bag = (DataBag) t.get(0);
    for (Tuple tuple : bag) {
      summaryData.addTuple(schema, tuple);
    }
    return summaryData;
  }

  @Override
  public String exec(Tuple t) throws IOException {
    return SummaryData.toPrettyJSON(sumUp(getInputSchema(), t));
  }

  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return Intermediate.class.getName();
  }

  @Override
  public String getFinal() {
    return Final.class.getName();
  }
}
