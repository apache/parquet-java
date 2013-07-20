package parquet.hadoop.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import parquet.column.Encoding;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ColumnChunkProperties {

  private static Map<ColumnChunkProperties, ColumnChunkProperties> cache = new HashMap<ColumnChunkProperties, ColumnChunkProperties>();

  public static ColumnChunkProperties get(ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec, Set<Encoding> encodings) {
    ColumnChunkProperties key = new ColumnChunkProperties(codec, path, type, encodings);
    ColumnChunkProperties cached = cache.get(key);
    if (cached == null) {
      cached = key;
      cache.put(key, cached);
    }
    return cached;
  }

  private final CompressionCodecName codec;
  private final ColumnPath path;
  private final PrimitiveTypeName type;
  private final Set<Encoding> encodings;

  private ColumnChunkProperties(CompressionCodecName codec, ColumnPath path,
      PrimitiveTypeName type, Set<Encoding> encodings) {
    super();
    this.codec = codec;
    this.path = path;
    this.type = type;
    this.encodings = encodings;
  }

  public CompressionCodecName getCodec() {
    return codec;
  }

  public ColumnPath getPath() {
    return path;
  }

  public PrimitiveTypeName getType() {
    return type;
  }

  public Set<Encoding> getEncodings() {
    return encodings;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ColumnChunkProperties) {
      ColumnChunkProperties other = (ColumnChunkProperties)obj;
      return other.codec == codec && other.path.equals(path) && other.type == type && equals(other.encodings, encodings);
    }
    return false;
  }


  private boolean equals(Set<Encoding> a, Set<Encoding> b) {
    return a.size() == b.size() && a.containsAll(b);
  }

  @Override
  public int hashCode() {
    return codec.hashCode() ^ path.hashCode() ^ type.hashCode() ^ Arrays.hashCode(encodings.toArray());
  }

  @Override
  public String toString() {
    return codec + " " + path + " " + type + "  " + encodings;
  }
}
