package parquet.thrift.struct;

import org.apache.thrift.TBase;
import org.apache.thrift.TUnion;

public class TBaseUtil {
  private TBaseUtil() {}

  public static <T extends TBase<?,?>> boolean isUnion(Class<T> klass) {
    return TUnion.class.isAssignableFrom(klass);
  }
}
