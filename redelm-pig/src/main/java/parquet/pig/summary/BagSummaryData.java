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

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * summary data for a bag
 *
 * @author Julien Le Dem
 *
 */
public class BagSummaryData extends SummaryData {

  private ValueStat size = new ValueStat();

  private FieldSummaryData content;

  /**
   * add a bag to the summary data
   *
   * @param bag
   */
  public void add(Schema schema, DataBag bag) {
    super.add(bag);
    size.add(bag.size());
    FieldSchema field = getField(schema, 0);
    if (bag.size() > 0 && content == null) {
      content = new FieldSummaryData();
      content.setName(getName(field));
    }
    for (Tuple tuple : bag) {
      content.add(getSchema(field), tuple);
    }
  }

  @Override
  public void merge(SummaryData other) {
    super.merge(other);
    BagSummaryData otherBagSummary = (BagSummaryData) other;
    size.merge(otherBagSummary.size);
    content = merge(content, otherBagSummary.content);
  }

  public FieldSummaryData getContent() {
    return content;
  }

  public void setContent(FieldSummaryData content) {
    this.content = content;
  }

  public ValueStat getSize() {
    return size;
  }

  public void setSize(ValueStat size) {
    this.size = size;
  }

}
