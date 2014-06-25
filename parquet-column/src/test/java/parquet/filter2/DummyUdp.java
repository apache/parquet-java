package parquet.filter2;


public class DummyUdp extends UserDefinedPredicates.IntUserDefinedPredicate {
  @Override
  public boolean keep(int value) {
    return true;
  }

  @Override
  public boolean canDrop(int min, int max) {
    return false;
  }

  @Override
  public boolean inverseCanDrop(int min, int max) {
    return false;
  }
}
