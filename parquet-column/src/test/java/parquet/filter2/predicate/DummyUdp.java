package parquet.filter2.predicate;

import java.io.Serializable;

public class DummyUdp extends UserDefinedPredicate<Integer> {

  @Override
  public boolean keep(Integer value) {
    return false;
  }

  @Override
  public boolean canDrop(Statistics<Integer> statistics) {
    return false;
  }

  @Override
  public boolean inverseCanDrop(Statistics<Integer> statistics) {
    return false;
  }
}
