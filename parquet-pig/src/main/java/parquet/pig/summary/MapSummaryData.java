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

import java.util.Map;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Summary data for a Map
 *
 * @author Julien Le Dem
 *
 */
public class MapSummaryData extends SummaryData {

  private ValueStat size = new ValueStat();

  private FieldSummaryData key;
  private FieldSummaryData value;

  /**
   * add a map to the summary
   * @param m the map
   */
  public void add(Schema schema, Map<?, ?> m) {
    super.add(m);
    size.add(m.size());
    FieldSchema field = getField(schema, 0);
    if (m.size() > 0 && key == null) {
      key = new FieldSummaryData();
      key.setName(getName(field));
      value = new FieldSummaryData();
      value.setName(getName(field));
    }
    for (Map.Entry<?, ?> entry : m.entrySet()) {
      key.add(null, entry.getKey());
      value.add(getSchema(field), entry.getValue());
    }
  }

  @Override
  public void merge(SummaryData other) {
    super.merge(other);
    MapSummaryData otherMapSummaryData = (MapSummaryData) other;
    size.merge(otherMapSummaryData.size);
    key = merge(key, otherMapSummaryData.key);
    value = merge(value, otherMapSummaryData.value);
  }

  public FieldSummaryData getKey() {
    return key;
  }

  public void setKey(FieldSummaryData key) {
    this.key = key;
  }

  public FieldSummaryData getValue() {
    return value;
  }

  public void setValue(FieldSummaryData value) {
    this.value = value;
  }

  public ValueStat getSize() {
    return size;
  }

  public void setSize(ValueStat size) {
    this.size = size;
  }

}
