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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class EnumStat {

  private static final int MAX_COUNT = 1000;

  public static class EnumValueCount {
    private String value;
    private int count;

    public EnumValueCount() {
    }

    public EnumValueCount(String value) {
      this.value = value;
    }

    public void add() {
      ++ count;
    }

    public String getValue() {
      return value;
    }
    public void setValue(String value) {
      this.value = value;
    }

    public int getCount() {
      return count;
    }
    public void setCount(int count) {
      this.count = count;
    }

    public void add(int countToAdd) {
      this.count += countToAdd;
    }

  }

  private Map<String, EnumValueCount> values = new HashMap<String, EnumValueCount>();

  public void add(String value) {
    if (values != null) {
      EnumValueCount enumValueCount = values.get(value);
      if (enumValueCount == null) {
        enumValueCount = new EnumValueCount(value);
        values.put(value, enumValueCount);
      }
      enumValueCount.add();
      checkValues();
    }
  }

  public void merge(EnumStat other) {
    if (values != null) {
      if (other.values == null) {
        values = null;
        return;
      }
      for (EnumValueCount otherValue : other.getValues()) {
        EnumValueCount myValue = values.get(otherValue.value);
        if (myValue == null) {
          values.put(otherValue.value, otherValue);
        } else {
          myValue.add(otherValue.count);
        }
      }
      checkValues();
    }
  }

  private void checkValues() {
    if (values.size() > MAX_COUNT) {
      values = null;
    }
  }

  public Collection<EnumValueCount> getValues() {
    return values == null ? null : values.values();
  }

  public void setValues(Collection<EnumValueCount> values) {
    if (values == null) {
      this.values = null;
    } else if (this.values != null) {
      for (EnumValueCount value : values) {
        this.values.put(value.getValue(), value);
      }
    }
  }
}
