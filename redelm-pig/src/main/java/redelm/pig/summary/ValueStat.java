package redelm.pig.summary;

public class ValueStat {

  private double total;
  private double min = Double.MAX_VALUE;
  private double max = Double.MIN_VALUE;

  public void add(double v) {
    total += v;
    min = Math.min(min, v);
    max = Math.max(max, v);
  }

  public void merge(ValueStat other) {
    total += other.total;
    min = Math.min(min, other.min);
    max = Math.max(max, other.max);
  }

  public double getTotal() {
    return total;
  }

  public void setTotal(double total) {
    this.total = total;
  }

  public double getMin() {
    return min;
  }

  public void setMin(double min) {
    this.min = min;
  }

  public double getMax() {
    return max;
  }

  public void setMax(double max) {
    this.max = max;
  }


}