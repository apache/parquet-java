package redelm.pig.converter;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class BagConverter extends Converter {
  private static final BagFactory BF = BagFactory.getInstance();

  private DataBag currentBag;
  private Converter child;

  @Override
  public void start() {
    currentBag = BF.newDefaultBag();
  }

  @Override
  public Converter end() {
    return getParent();
  }

  @Override
  public void startField(String field, int index) {
    assert index == 0;
  }

  @Override
  public void endField(String field, int index) {
    assert index == 0;
  }

  @Override
  public Converter startGroup() {
    return child;
  }

  @Override
  public void endGroup() {
    currentBag.add((Tuple)child.get());
  }

  @Override
  public Object get() {
    return currentBag;
  }

  @Override
  public void set(Object value) {
    throw new RuntimeException("bag can not contain primitive value "+value);
  }

}
