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

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * summary data for one field of a tuple
 * usually only one the *Summary member if set
 *
 * @author Julien Le Dem
 *
 */
public class FieldSummaryData extends SummaryData {

  private String name;

  private BagSummaryData bag;
  private TupleSummaryData tuple;
  private MapSummaryData map;
  private StringSummaryData string;
  private NumberSummaryData number;

  private long nullCount;
  private long unknown;
  private long error;

  @Override
  public void merge(SummaryData other) {
    super.merge(other);
    FieldSummaryData otherFieldSummaryData = (FieldSummaryData) other;

    if (otherFieldSummaryData.name != null) {
      setName(otherFieldSummaryData.name);
    }

    bag = merge(bag, otherFieldSummaryData.bag);
    tuple = merge(tuple, otherFieldSummaryData.tuple);
    map = merge(map, otherFieldSummaryData.map);
    string = merge(string, otherFieldSummaryData.string);
    number = merge(number, otherFieldSummaryData.number);

    nullCount += otherFieldSummaryData.nullCount;
    unknown += otherFieldSummaryData.unknown;
    error += otherFieldSummaryData.error;
  }

  /**
   * add an object to the summary data
   */
  public void add(Schema schema, Object o) {
    super.add(o);
    if (o == null) {
      ++nullCount;
    } else if (o instanceof DataBag) {
      if (bag == null) {
        bag = new BagSummaryData();
      }
      bag.add(schema, (DataBag) o);
    } else if (o instanceof Tuple) {
      if (tuple == null) {
        tuple = new TupleSummaryData();
      }
      tuple.addTuple(schema, (Tuple) o);
    } else if (o instanceof Map<?, ?>) {
      if (map == null) {
        map = new MapSummaryData();
      }
      map.add(schema, (Map<?, ?>) o);
    } else if (o instanceof String) {
      if (string == null) {
        string = new StringSummaryData();
      }
      string.add((String) o);
    } else if (o instanceof Number) {
      if (number == null) {
        number = new NumberSummaryData();
      }
      number.add((Number) o);
    } else {
      ++unknown;
    }
  }

  public void addError() {
    ++error;
  }

  public BagSummaryData getBag() {
    return bag;
  }

  public void setBag(BagSummaryData bag) {
    this.bag = bag;
  }

  public TupleSummaryData getTuple() {
    return tuple;
  }

  public void setTuple(TupleSummaryData tuple) {
    this.tuple = tuple;
  }

  public MapSummaryData getMap() {
    return map;
  }

  public void setMap(MapSummaryData map) {
    this.map = map;
  }

  public StringSummaryData getString() {
    return string;
  }

  public void setString(StringSummaryData string) {
    this.string = string;
  }

  public NumberSummaryData getNumber() {
    return number;
  }

  public void setNumber(NumberSummaryData number) {
    this.number = number;
  }

  public Long getNull() {
    return nullCount == 0 ? null : nullCount;
  }

  public void setNull(long nullCnt) {
    this.nullCount = nullCnt;
  }

  public Long getUnknown() {
    return unknown == 0 ? null : unknown;
  }

  public void setUnknown(long unknown) {
    this.unknown = unknown;
  }

  public Long getError() {
    return error == 0 ? null : error;
  }

  public void setError(long error) {
    this.error = error;
  }

  public void setName(String name) {
    if (this.name != null && !this.name.equals(name)) {
      throw new IllegalStateException("name mismatch " + this.name + " expected, got " + name);
    }
    this.name = name;
  }

  public String getName() {
    return name;
  }

}
