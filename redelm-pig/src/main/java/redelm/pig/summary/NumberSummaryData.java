package redelm.pig.summary;

/**
 * Summary data for a Number
 *
 * @author julien
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
