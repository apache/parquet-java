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

import org.junit.Test;

import static org.junit.Assert.*;

public class TestLruCache {
  private static final String DEFAULT_KEY = "test";

  private static final class SimpleValue implements LruCache.Value<String, SimpleValue> {
    private boolean current;
    private boolean newerThan;

    public SimpleValue(boolean current, boolean newerThan) {
      this.current = current;
      this.newerThan = newerThan;
    }

    @Override
    public boolean isCurrent(String key) {
      return current;
    }

    public void setCurrent(boolean current) {
      this.current = current;
    }

    @Override
    public boolean isNewerThan(SimpleValue otherValue) {
      return newerThan;
    }

  }

  @Test
  public void testMaxSize() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    String oldKey = DEFAULT_KEY;
    String newKey = oldKey + "_new";

    SimpleValue oldValue = new SimpleValue(true, true);
    cache.put(oldKey, oldValue);
    assertEquals(oldValue, cache.getCurrentValue(oldKey));
    assertEquals(1, cache.size());

    SimpleValue newValue = new SimpleValue(true, true);
    cache.put(newKey, newValue);
    assertNull(cache.getCurrentValue(oldKey));
    assertEquals(newValue, cache.getCurrentValue(newKey));
    assertEquals(1, cache.size());
  }

  @Test
  public void testOlderValueIsIgnored() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue currentValue = new SimpleValue(true, true);
    SimpleValue notAsCurrentValue = new SimpleValue(true, false);
    cache.put(DEFAULT_KEY, currentValue);
    cache.put(DEFAULT_KEY, notAsCurrentValue);
    assertEquals(
            "The existing value in the cache was overwritten",
            currentValue,
            cache.getCurrentValue(DEFAULT_KEY)
    );
  }

  @Test
  public void testOutdatedValueIsIgnored() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue outdatedValue = new SimpleValue(false, true);
    cache.put(DEFAULT_KEY, outdatedValue);
    assertEquals(0, cache.size());
    assertNull(cache.getCurrentValue(DEFAULT_KEY));
  }

  @Test
  public void testCurrentValueOverwritesExisting() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue currentValue = new SimpleValue(true, true);
    SimpleValue notAsCurrentValue = new SimpleValue(true, false);
    cache.put(DEFAULT_KEY, notAsCurrentValue);
    assertEquals(1, cache.size());
    cache.put(DEFAULT_KEY, currentValue);
    assertEquals(1, cache.size());
    assertEquals(
            "The existing value in the cache was NOT overwritten",
            currentValue,
            cache.getCurrentValue(DEFAULT_KEY)
    );
  }

  @Test
  public void testGetOutdatedValueReturnsNull() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue value = new SimpleValue(true, true);
    cache.put(DEFAULT_KEY, value);
    assertEquals(1, cache.size());
    assertEquals(value, cache.getCurrentValue(DEFAULT_KEY));

    value.setCurrent(false);
    assertNull("The value should not be current anymore", cache.getCurrentValue(DEFAULT_KEY));
    assertEquals(0, cache.size());
  }

  @Test
  public void testRemove() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue value = new SimpleValue(true, true);
    cache.put(DEFAULT_KEY, value);
    assertEquals(1, cache.size());
    assertEquals(value, cache.getCurrentValue(DEFAULT_KEY));

    // remove the only value
    assertEquals(value, cache.remove(DEFAULT_KEY));
    assertNull(cache.getCurrentValue(DEFAULT_KEY));
    assertEquals(0, cache.size());
  }

  @Test
  public void testClear() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(2);

    String key1 = DEFAULT_KEY + 1;
    String key2 = DEFAULT_KEY + 2;
    SimpleValue value = new SimpleValue(true, true);
    cache.put(key1, value);
    cache.put(key2, value);
    assertEquals(value, cache.getCurrentValue(key1));
    assertEquals(value, cache.getCurrentValue(key2));
    assertEquals(2, cache.size());

    cache.clear();
    assertNull(cache.getCurrentValue(key1));
    assertNull(cache.getCurrentValue(key2));
    assertEquals(0, cache.size());
  }

}
