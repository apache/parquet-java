package parquet;

public abstract class Either<L, R> {

  public static <L, R> Left<L, R> left(L left) {
    return new Left<L, R>(left);
  }

  public static <L, R> Right<L, R> right(R right) {
    return new Right<L, R>(right);
  }

  public abstract boolean isLeft();
  public abstract Left<L, R> asLeft();
  public abstract boolean isRight();
  public abstract Right<L, R> asRight();

  public static class Left<L, R> extends Either<L, R> {
    private final L left;

    private Left(L left) {
      this.left = left;
    }

    public L get() {
      return left;
    }

    @Override
    public boolean isLeft() {
      return true;
    }

    @Override
    public Left<L, R> asLeft() {
      return this;
    }

    @Override
    public boolean isRight() {
      return false;
    }

    @Override
    public Right<L, R> asRight() {
      throw new UnsupportedOperationException("asRight() called on a Left!");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Left left1 = (Left) o;

      return !(left != null ? !left.equals(left1.left) : left1.left != null);
    }

    @Override
    public int hashCode() {
      return (31 * getClass().hashCode()) + (left != null ? left.hashCode() : 0);
    }
  }

  public static class Right<L, R> extends Either<L, R> {
    private final R right;

    public Right(R right) {
      this.right = right;
    }

    public R get() {
      return right;
    }

    @Override
    public boolean isLeft() {
      return false;
    }

    @Override
    public Left<L, R> asLeft() {
      throw new UnsupportedOperationException("asLeft() called on a Right!");
    }

    @Override
    public boolean isRight() {
      return true;
    }

    @Override
    public Right<L, R> asRight() {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Right right1 = (Right) o;

      return !(right != null ? !right.equals(right1.right) : right1.right != null);
    }

    @Override
    public int hashCode() {
      return (31 * getClass().hashCode()) + (right != null ? right.hashCode() : 0);
    }
  }

}
