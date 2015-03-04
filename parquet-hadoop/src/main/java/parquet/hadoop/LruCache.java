/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.hadoop;

import parquet.Log;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A basic implementation of an LRU cache.  Besides evicting the least recently
 * used entries (either based on insertion or access order), this class also
 * checks for "stale" entries as entries are inserted or retrieved (note
 * "staleness" is defined by the entries themselves (see
 * {@link parquet.hadoop.LruCache.Value}).
 *
 * @param <K> The key type. Acts as the key in a {@link java.util.LinkedHashMap}
 * @param <V> The value type.  Must extend {@link parquet.hadoop.LruCache.Value}
 *           so that the "staleness" of the value can be easily determined.
 */
final class LruCache<K, V extends LruCache.Value<K, V>> {
  private static final Log LOG = Log.getLog(LruCache.class);

  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  private final LinkedHashMap<K, V> cacheMap;

  /**
   * Constructs an access-order based LRU cache with {@code maxSize} entries.
   * @param maxSize The maximum number of entries to store in the cache.
   */
  public LruCache(final int maxSize) {
    this(maxSize, DEFAULT_LOAD_FACTOR, true);
  }

  /**
   * Constructs an LRU cache.
   *
   * @param maxSize The maximum number of entries to store in the cache.
   * @param loadFactor Used to determine the initial capacity.
   * @param accessOrder the ordering mode - {@code true} for access-order,
   * {@code false} for insertion-order
   */
  public LruCache(final int maxSize, final float loadFactor, final boolean accessOrder) {
    int initialCapacity = Math.round(maxSize / loadFactor);
    cacheMap =
            new LinkedHashMap<K, V>(initialCapacity, loadFactor, accessOrder) {
              @Override
              public boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
                boolean result = size() > maxSize;
                if (result) {
                  if (Log.DEBUG) {
                    LOG.debug("Removing eldest entry in cache: "
                            + eldest.getKey());
                  }
                }
                return result;
              }
            };
  }

  /**
   * Removes the mapping for the specified key from this cache if present.
   * @param key key whose mapping is to be removed from the cache
   * @return the previous value associated with key, or null if there was no
   * mapping for key.
   */
  public V remove(final K key) {
    V oldValue = cacheMap.remove(key);
    if (oldValue != null) {
      if (Log.DEBUG) {
        LOG.debug("Removed cache entry for '" + key + "'");
      }
    }
    return oldValue;
  }

  /**
   * Associates the specified value with the specified key in this cache. The
   * value is only inserted if it is not null and it is considered current. If
   * the cache previously contained a mapping for the key, the old value is
   * replaced only if the new value is "newer" than the old one.
   * @param key key with which the specified value is to be associated
   * @param newValue value to be associated with the specified key
   */
  public void put(final K key, final V newValue) {
    if (newValue == null || !newValue.isCurrent(key)) {
      if (Log.WARN) {
        LOG.warn("Ignoring new cache entry for '" + key + "' because it is "
                + (newValue == null ? "null" : "not current"));
      }
      return;
    }

    V oldValue = cacheMap.get(key);
    if (oldValue != null && oldValue.isNewerThan(newValue)) {
      if (Log.WARN) {
        LOG.warn("Ignoring new cache entry for '" + key + "' because "
                + "existing cache entry is newer");
      }
      return;
    }

    // no existing value or new value is newer than old value
    oldValue = cacheMap.put(key, newValue);
    if (Log.DEBUG) {
      if (oldValue == null) {
        LOG.debug("Added new cache entry for '" + key + "'");
      } else {
        LOG.debug("Overwrote existing cache entry for '" + key + "'");
      }
    }
  }

  /**
   * Removes all of the mappings from this cache. The cache will be empty
   * after this call returns.
   */
  public void clear() {
    cacheMap.clear();
  }

  /**
   * Returns the value to which the specified key is mapped, or null if 1) the
   * value is not current or 2) this cache contains no mapping for the key.
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is mapped, or null if 1) the
   * value is not current or 2) this cache contains no mapping for the key
   */
  public V getCurrentValue(final K key) {
    V value = cacheMap.get(key);
    if (Log.DEBUG) {
      LOG.debug("Value for '" + key + "' " + (value == null ? "not " : "")
              + "in cache");
    }
    if (value != null && !value.isCurrent(key)) {
      // value is not current; remove it and return null
      remove(key);
      return null;
    }

    return value;
  }

  /**
   * Returns the number of key-value mappings in this cache.
   * @return the number of key-value mappings in this cache.
   */
  public int size() {
    return cacheMap.size();
  }

  /**
   * {@link parquet.hadoop.LruCache} expects all values to follow this
   * interface so the cache can determine 1) whether values are current (e.g.
   * the referenced data has not been modified/updated in such a way that the
   * value is no longer useful) and 2) whether a value is strictly "newer"
   * than another value.
   *
   * @param <K> The key type.
   * @param <V> Provides a bound for the {@link #isNewerThan(V)} method
   */
  interface Value<K, V> {
    /**
     * Is the value still current (e.g. has the referenced data been
     * modified/updated in such a way that the value is no longer useful)
     * @param key the key associated with this value
     * @return {@code true} the value is still current, {@code false} the value
     * is no longer useful
     */
    boolean isCurrent(K key);

    /**
     * Compares this value with the specified value to check for relative age.
     * @param otherValue the value to be compared.
     * @return {@code true} the value is strictly newer than the other value,
     * {@code false} the value is older or just
     * as new as the other value.
     */
    boolean isNewerThan(V otherValue);
  }

}
