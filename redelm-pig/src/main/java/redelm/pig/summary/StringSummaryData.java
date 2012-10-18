package redelm.pig.summary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.codehaus.jackson.annotate.JsonWriteNullProperties;

import redelm.pig.summary.EnumStat.EnumValueCount;

/**
 * Summary data for a String
 *
 * @author julien
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
