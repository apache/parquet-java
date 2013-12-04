package parquet.hadoop.codec;

public class CompressionCodecNotSupportedException extends RuntimeException {
  private final Class codecClass;

  public CompressionCodecNotSupportedException(Class codecClass) {
    super("codec not supported: " + codecClass.getName());
    this.codecClass = codecClass;
  }

  public Class getCodecClass() {
    return codecClass;
  }
}
