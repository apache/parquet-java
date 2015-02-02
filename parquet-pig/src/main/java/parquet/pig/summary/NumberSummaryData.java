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

/**
 * Summary data for a Number
 *
 * @author Julien Le Dem
 *
 */
public class NumberSummaryData extends SummaryData {

  private ValueStat value = new ValueStat();

  public void add(Number n) {
    super.add(n);
    value.add(n.doubleValue());
  }

  @Override
  public void merge(SummaryData other) {
    super.merge(other);
    value.merge(((NumberSummaryData) other).value);
  }

  public ValueStat getValue() {
    return value;
  }

  public void setValue(ValueStat value) {
    this.value = value;
  }

}
