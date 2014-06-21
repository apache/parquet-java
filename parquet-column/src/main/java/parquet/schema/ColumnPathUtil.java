package parquet.schema;

import java.util.Arrays;
import java.util.Iterator;

public final class ColumnPathUtil {
  private ColumnPathUtil() { }

  public static String toDotSeparatedString(String[] path) {
    StringBuilder sb = new StringBuilder();
    Iterator<String> iter = Arrays.asList(path).iterator();
    while(iter.hasNext()) {
      sb.append(iter.next());

      if (iter.hasNext()) {
        sb.append('.');
      }
    }
    return sb.toString();
  }
}
