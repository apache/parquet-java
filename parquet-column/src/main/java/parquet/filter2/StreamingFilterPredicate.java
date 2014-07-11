package parquet.filter2;

import parquet.Preconditions;
import parquet.io.api.Binary;

public interface StreamingFilterPredicate {
  public static interface Visitor {
    boolean visit(Atom p);
    boolean visit(And and);
    boolean visit(Or or);
  }

  boolean accept(Visitor visitor);

  public static abstract class Atom implements StreamingFilterPredicate {
    private boolean result = false;
    private boolean isKnown = false;

    public final void reset() {
      isKnown = false;
      result = false;
    }

    public void updateNull() { throw new UnsupportedOperationException(); }
    public void update(int value) { throw new UnsupportedOperationException(); }
    public void update(long value) { throw new UnsupportedOperationException(); }
    public void update(double value) { throw new UnsupportedOperationException(); }
    public void update(float value) { throw new UnsupportedOperationException(); }
    public void update(boolean value) { throw new UnsupportedOperationException(); }
    public void update(Binary value) { throw new UnsupportedOperationException(); }

    protected void setResult(boolean result) {
      this.result = result;
      this.isKnown = true;
    }

    public boolean getResult() {
      if (!isKnown) {
        throw new IllegalStateException("getResult() called on an Atom whose result is not yet known!");
      }
      return result;
    }

    public boolean isKnown() {
      return isKnown;
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static abstract class BinaryLogical implements StreamingFilterPredicate {
    private final StreamingFilterPredicate left;
    private final StreamingFilterPredicate right;

    public BinaryLogical(StreamingFilterPredicate left, StreamingFilterPredicate right) {
      this.left = Preconditions.checkNotNull(left, "left");
      this.right = Preconditions.checkNotNull(right, "right");
    }

    public StreamingFilterPredicate getLeft() {
      return left;
    }

    public StreamingFilterPredicate getRight() {
      return right;
    }
  }

  public static class Or extends BinaryLogical {
    public Or(StreamingFilterPredicate left, StreamingFilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static class And extends BinaryLogical {
    public And(StreamingFilterPredicate left, StreamingFilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }
}
