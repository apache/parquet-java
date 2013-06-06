package parquet;

/**
 * Utilities for working with ints
 *
 * @author Alex Levenson
 */
public final class Ints {
  private Ints() { }

  /**
   * Cast value to a an int, or throw an exception
   * if there is an overflow.
   *
   * @param value a long to be casted to an int
   * @return an int that is == to value
   * @throws IllegalArgumentException if value can't be casted to an int
   */
  public static int checkedCast(long value) {
    int valueI = (int) value;
    if (valueI != value) {
      throw new IllegalArgumentException(String.format("Overflow casting %d to an int", value));
    }
    return valueI;
  }
}
