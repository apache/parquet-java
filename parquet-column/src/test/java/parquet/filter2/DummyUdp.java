package parquet.filter2;

public class DummyUdp extends UserDefinedPredicate<Integer> {

  @Override
  public boolean keep(Integer value) {
    return false;
  }

  @Override
  public boolean canDrop(Integer min, Integer max) {
    return false;
  }

  @Override
  public boolean inverseCanDrop(Integer min, Integer max) {
    return false;
  }
}
