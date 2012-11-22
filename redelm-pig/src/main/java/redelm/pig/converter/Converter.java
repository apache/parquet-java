package redelm.pig.converter;

public abstract class Converter {

  private Converter parent;

  Converter(Converter parent) {
    super();
    this.parent = parent;
  }

  abstract public void start();

  public Converter end() {
    return parent;
  }

  abstract public void startField(String field, int index);

  abstract public void endField(String field, int index);

  public Converter getParent() {
    return parent;
  }

  abstract public Converter startGroup();

  abstract public void endGroup();

  abstract public Object get();

  abstract public void set(Object value);

}
