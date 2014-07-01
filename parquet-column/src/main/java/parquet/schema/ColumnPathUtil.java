package parquet.schema;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Utilities for working with ColumnPaths and String arrays that are column paths
 *
 * TODO(alexlevenson): is this needed? Or should ColumnPaths in the filter2 api
 * TODO(alexlevenson): be stored as canonicalized ColumnPaths instead?
 */
public final class ColumnPathUtil {
  private ColumnPathUtil() { }

  /**
   * "joins" an array of string with the '.' character, eg:
   * ["a","b",c"] -> "a.b.c"
   */
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
