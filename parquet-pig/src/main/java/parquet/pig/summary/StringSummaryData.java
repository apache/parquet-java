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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.codehaus.jackson.annotate.JsonWriteNullProperties;

import parquet.pig.summary.EnumStat.EnumValueCount;


/**
 * Summary data for a String
 *
 * @author Julien Le Dem
 *
 */
@JsonWriteNullProperties(value = false)
public class StringSummaryData extends SummaryData {

  private ValueStat size = new ValueStat();
  private EnumStat values = new EnumStat();

  public void add(String s) {
    super.add(s);
    size.add(s.length());
    values.add(s);
  }

  @Override
  public void merge(SummaryData other) {
    super.merge(other);
    StringSummaryData stringSummaryData = (StringSummaryData) other;
    size.merge(stringSummaryData.size);
    values.merge(stringSummaryData.values);
  }

  public ValueStat getSize() {
    return size;
  }

  public void setSize(ValueStat size) {
    this.size = size;
  }

  public Collection<EnumValueCount> getValues() {
    Collection<EnumValueCount> values2 = values.getValues();
    if (values2 == null) {
      return null;
    }
    List<EnumValueCount> list = new ArrayList<EnumValueCount>(values2);
    Collections.sort(list, new Comparator<EnumValueCount>() {
      @Override
      public int compare(EnumValueCount o1, EnumValueCount o2) {
        return o2.getCount() - o1.getCount();
      }
    });
    return list;
  }

  public void setValues(Collection<EnumValueCount> values) {
    this.values.setValues(values);
  }

}
