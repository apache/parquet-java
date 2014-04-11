package parquet.schema;

public class OriginalTypeMeta {
  private final int precision;
  private final int scale;

  public OriginalTypeMeta(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OriginalTypeMeta that = (OriginalTypeMeta) o;

    if (precision != that.precision) return false;
    if (scale != that.scale) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = precision;
    result = 31 * result + scale;
    return result;
  }
}
