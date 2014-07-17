package parquet;

public class Optional<T> {
  private final T t;
  private Optional(T t) {
    this.t = t;
  }

  public boolean isPresent() {
    return t != null;
  }

  public T get() {
    if (t == null) {
      throw new IllegalStateException("get() called on an Optional.absent()");
    }
    return t;
  }

  @Override
  public String toString() {
    if (isPresent()) {
      return "Optional.of(" + t + ")";
    } else {
      return  "Optional.absent()";
    }
  }

  public static <T> Optional<T> of(T t) {
    if (t == null) {
      throw new IllegalArgumentException("Cannont construct an Optional.of() from null");
    }
    return new Optional<T>(t);
  }

  private static final Optional<?> ABSENT = new Optional<Object>(null);

  @SuppressWarnings("unchecked")
  public static <T> Optional<T> absent() {
    return (Optional<T>) ABSENT;
  }

  public static <T> Optional<T> fromNullable(T t) {
    if (t == null) {
      return absent();
    }
    return of(t);
  }

}
