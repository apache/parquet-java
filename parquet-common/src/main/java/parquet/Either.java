package parquet;

public abstract class Either<L, R> {

  public static <L, R> Either<L, R> left(L left) {
    Preconditions.checkNotNull(left, "left");
    return new Left<L, R>(left);
  }

  public static <L, R> Either<L, R> right(R right) {
    Preconditions.checkNotNull(right, "right");
    return new Right<L, R>(right);
  }

  public abstract boolean isLeft();
  public abstract L asLeft();
  public abstract boolean isRight();
  public abstract R asRight();

  public static final class Left<L, R> extends Either<L, R> {
    private final L left;

    private Left(L left) {
      this.left = left;
    }

    @Override
    public boolean isLeft() {
      return true;
    }

    @Override
    public L asLeft() {
      return left;
    }

    @Override
    public boolean isRight() {
      return false;
    }

    @Override
    public R asRight() {
      throw new UnsupportedOperationException("asRight() called on a Left!");
    }

    @Override
    public String toString() {
      return "Left(" + left + ")";
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

  public static final class Right<L, R> extends Either<L, R> {
    private final R right;

    public Right(R right) {
      this.right = right;
    }

    @Override
    public boolean isLeft() {
      return false;
    }

    @Override
    public L asLeft() {
      throw new UnsupportedOperationException("asLeft() called on a Right!");
    }

    @Override
    public boolean isRight() {
      return true;
    }

    @Override
    public R asRight() {
      return right;
    }

    @Override
    public String toString() {
      return "Right(" + right + ")";
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
