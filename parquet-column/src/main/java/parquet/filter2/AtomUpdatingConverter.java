package parquet.filter2;

import parquet.Preconditions;
import parquet.column.Dictionary;
import parquet.filter2.StreamingFilterPredicate.Atom;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

public class AtomUpdatingConverter extends PrimitiveConverter {
  private final PrimitiveConverter delegate;
  private final Atom[] atoms;

  public AtomUpdatingConverter(PrimitiveConverter delegate, Atom[] atoms) {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    this.atoms = Preconditions.checkNotNull(atoms, "atoms");
  }

  @Override
  public boolean hasDictionarySupport() {
    return delegate.hasDictionarySupport();
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    delegate.setDictionary(dictionary);
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    // TODO: ????
    delegate.addValueFromDictionary(dictionaryId);
  }

  @Override
  public void addBinary(Binary value) {
    for (Atom atom : atoms) {
      atom.update(value);
    }
    delegate.addBinary(value);
  }

  @Override
  public void addBoolean(boolean value) {
    for (Atom atom : atoms) {
      atom.update(value);
    }
    delegate.addBoolean(value);
  }

  @Override
  public void addDouble(double value) {
    for (Atom atom : atoms) {
      atom.update(value);
    }
    delegate.addDouble(value);
  }

  @Override
  public void addFloat(float value) {
    for (Atom atom : atoms) {
      atom.update(value);
    }
    delegate.addFloat(value);
  }

  @Override
  public void addInt(int value) {
    for (Atom atom : atoms) {
      atom.update(value);
    }
    delegate.addInt(value);
  }

  @Override
  public void addLong(long value) {
    for (Atom atom : atoms) {
      atom.update(value);
    }
    delegate.addLong(value);
  }
}
