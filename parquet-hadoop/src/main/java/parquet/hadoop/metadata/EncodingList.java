package parquet.hadoop.metadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import parquet.column.Encoding;

public class EncodingList implements Iterable<Encoding> {

  private static Map<EncodingList, EncodingList> encodingLists = new HashMap<EncodingList, EncodingList>();

  public static EncodingList getEncodingList(List<Encoding> encodings) {
    EncodingList key = new EncodingList(encodings);
    EncodingList cached = encodingLists.get(key);
    if (cached == null) {
      cached = key;
      encodingLists.put(key, cached);
    }
    return cached;
  }

  private final List<Encoding> encodings;

  private EncodingList(List<Encoding> encodings) {
    super();
    this.encodings = Collections.unmodifiableList(encodings);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EncodingList) {
      List<parquet.column.Encoding> other = ((EncodingList)obj).encodings;
      final int size = other.size();
      if (size != encodings.size()) {
        return false;
      }
      for (int i = 0; i < size; i++) {
        if (!other.get(i).equals(encodings.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 1;
    for (parquet.column.Encoding element : encodings)
      result = 31 * result + (element == null ? 0 : element.hashCode());
    return result;
  }

  public List<Encoding> toList() {
    return encodings;
  }

  @Override
  public Iterator<Encoding> iterator() {
    return encodings.iterator();
  }

  public int size() {
    return encodings.size();
  }

}
