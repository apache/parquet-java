package parquet.hadoop;

import parquet.Log;

import java.util.LinkedHashMap;
import java.util.Map;

final class LruCache<K, V extends LruCache.Entry<V>> {
  private static final Log LOG = Log.getLog(LruCache.class);

  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  private final LinkedHashMap<K, V> cacheMap;

  public LruCache(int maxSize) {
    this(maxSize, DEFAULT_LOAD_FACTOR, true);
  }

  public LruCache(final int maxSize, float loadFactor, boolean accessOrder) {
    cacheMap =
            new LinkedHashMap<K, V>(Math.round(maxSize / loadFactor), loadFactor, accessOrder) {
              @Override
              public boolean removeEldestEntry(Map.Entry<K,V> eldest) {
                boolean result = size() > maxSize;
                if (result) {
                  if (Log.DEBUG) LOG.debug("Removing eldest entry in cache: " + eldest.getKey());
                }
                return result;
              }
            };
  }

  public V remove(K key) {
    V oldEntry = cacheMap.remove(key);
    if (oldEntry != null) {
      if (Log.DEBUG) LOG.debug("Removed cache entry for '" + key + "'");
    }
    return oldEntry;
  }

  public void put(K key, V newEntry) {
    if (newEntry == null || !newEntry.isCurrent()) {
      if (Log.WARN) LOG.warn("Ignoring new cache entry for '" + key + "' because it is " + (newEntry == null ? "null" : "not current"));
      return;
    }

    V oldEntry = cacheMap.get(key);
    if (oldEntry != null && oldEntry.isNewerThan(newEntry)) {
      if (Log.WARN) LOG.warn("Ignoring new cache entry for '" + key + "' because existing cache entry is newer");
      return;
    }

    // no existing entry or new entry is newer than old entry
    oldEntry = cacheMap.put(key, newEntry);
    if (Log.DEBUG) {
      if (oldEntry == null) {
        LOG.debug("Added new cache entry for '" + key + "'");
      } else {
        LOG.debug("Overwrote existing cache entry for '" + key + "'");
      }
    }
  }

  public void clear() {
    cacheMap.clear();
  }

  public V getCurrentEntry(K key) {
    V value = cacheMap.get(key);
    if (Log.DEBUG) LOG.debug("Entry for '" + key + "' " + (value == null ? "not " : "") + "in cache");
    if (value != null && !value.isCurrent()) {
      // value is not current; remove it and return null
      remove(key);
      return null;
    }

    return value;
  }

  public int size() {
    return cacheMap.size();
  }

  interface Entry<V> {
    public boolean isCurrent();
    public boolean isNewerThan(V otherEntry);
  }

}
