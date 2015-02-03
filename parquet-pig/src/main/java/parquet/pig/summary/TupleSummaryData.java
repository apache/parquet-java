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
package parquet.pig.summary;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Summary data for a Tuple
 * fields are in the same order as the input tuple
 *
 * @author Julien Le Dem
 *
 */
public class TupleSummaryData extends SummaryData {
  private static final Logger LOG = Logger.getLogger(TupleSummaryData.class.getName());

  private List<FieldSummaryData> fields = new ArrayList<FieldSummaryData>();

  private ValueStat size = new ValueStat();

  /**
   * add tuple to the summary
   *
   * @param tuple
   */
  public void addTuple(Schema schema, Tuple tuple) {
    super.add(tuple);
    int tupleSize = tuple.size();
    size.add(tupleSize);
    ensureSize(tupleSize);
    for (int i = 0; i < tupleSize; i++) {
      FieldSummaryData fieldSummaryData = fields.get(i);
      try {
        FieldSchema field = getField(schema, i);
        fieldSummaryData.setName(getName(field));
        Object o = tuple.get(i);
        fieldSummaryData.add(getSchema(field), o);
      } catch (ExecException e) {
        LOG.log(Level.WARNING, "Can't get value from tuple", e);
        fieldSummaryData.addError();
      }
    }
  }

  private void ensureSize(int sizeToEnsure) {
    while (fields.size() < sizeToEnsure) {
      fields.add(new FieldSummaryData());
    }
  }

  @Override
  public void merge(SummaryData other) {
    super.merge(other);
    TupleSummaryData otherTupleSummaryData = (TupleSummaryData) other;
    size.merge(otherTupleSummaryData.size);

    ensureSize(otherTupleSummaryData.fields.size());
    for (int i = 0; i < otherTupleSummaryData.fields.size(); i++) {
      fields.get(i).merge(otherTupleSummaryData.fields.get(i));
    }

  }

  public List<FieldSummaryData> getFields() {
    return fields;
  }

  public void setFields(List<FieldSummaryData> fields) {
    this.fields = fields;
  }

  public ValueStat getSize() {
    return size;
  }

  public void setSize(ValueStat size) {
    this.size = size;
  }


}

