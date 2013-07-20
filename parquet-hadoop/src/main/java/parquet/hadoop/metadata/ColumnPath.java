package parquet.hadoop.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class ColumnPath implements Iterable<String> {

  private static Map<ColumnPath, ColumnPath> paths = new HashMap<ColumnPath, ColumnPath>();

  public static ColumnPath get(String... path){
    ColumnPath key = new ColumnPath(path);
    ColumnPath cached = paths.get(key);
    if (cached == null) {
      for (int i = 0; i < path.length; i++) {
        path[i] = path[i].intern();
      }
      cached = key;
      paths.put(key, cached);
    }
    return cached;
  }

  private final String[] p;

  private ColumnPath(String[] path) {
    this.p = path;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ColumnPath) {
      return Arrays.equals(p, ((ColumnPath)obj).p);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(p);
  }

  @Override
  public String toString() {
    return Arrays.toString(p);
  }

  @Override
  public Iterator<String> iterator() {
    return Arrays.asList(p).iterator();
  }

  public int size() {
    return p.length;
  }

  public String[] toArray() {
    return p;
  }
}
