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
package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

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
    assertThat(cache.getCurrentValue(oldKey)).isEqualTo(oldValue);
    assertThat(cache.size()).isOne();

    SimpleValue newValue = new SimpleValue(true, true);
    cache.put(newKey, newValue);
    assertThat(cache.getCurrentValue(oldKey)).isNull();
    assertThat(cache.getCurrentValue(newKey)).isEqualTo(newValue);
    assertThat(cache.size()).isOne();
  }

  @Test
  public void testOlderValueIsIgnored() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue currentValue = new SimpleValue(true, true);
    SimpleValue notAsCurrentValue = new SimpleValue(true, false);
    cache.put(DEFAULT_KEY, currentValue);
    cache.put(DEFAULT_KEY, notAsCurrentValue);
    assertThat(cache.getCurrentValue(DEFAULT_KEY))
        .as("The existing value in the cache was overwritten")
        .isEqualTo(currentValue);
  }

  @Test
  public void testOutdatedValueIsIgnored() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue outdatedValue = new SimpleValue(false, true);
    cache.put(DEFAULT_KEY, outdatedValue);
    assertThat(cache.size()).isZero();
    assertThat(cache.getCurrentValue(DEFAULT_KEY)).isNull();
  }

  @Test
  public void testCurrentValueOverwritesExisting() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue currentValue = new SimpleValue(true, true);
    SimpleValue notAsCurrentValue = new SimpleValue(true, false);
    cache.put(DEFAULT_KEY, notAsCurrentValue);
    assertThat(cache.size()).isOne();
    cache.put(DEFAULT_KEY, currentValue);
    assertThat(cache.size()).isOne();
    assertThat(cache.getCurrentValue(DEFAULT_KEY))
        .as("The existing value in the cache was NOT overwritten")
        .isEqualTo(currentValue);
  }

  @Test
  public void testGetOutdatedValueReturnsNull() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue value = new SimpleValue(true, true);
    cache.put(DEFAULT_KEY, value);
    assertThat(cache.size()).isOne();
    assertThat(cache.getCurrentValue(DEFAULT_KEY)).isEqualTo(value);

    value.setCurrent(false);
    assertThat(cache.getCurrentValue(DEFAULT_KEY))
        .as("The value should not be current anymore")
        .isNull();
    assertThat(cache.size()).isZero();
  }

  @Test
  public void testRemove() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(1);

    SimpleValue value = new SimpleValue(true, true);
    cache.put(DEFAULT_KEY, value);
    assertThat(cache.size()).isOne();
    assertThat(cache.getCurrentValue(DEFAULT_KEY)).isEqualTo(value);

    // remove the only value
    assertThat(cache.remove(DEFAULT_KEY)).isEqualTo(value);
    assertThat(cache.getCurrentValue(DEFAULT_KEY)).isNull();
    assertThat(cache.size()).isZero();
  }

  @Test
  public void testClear() {
    LruCache<String, SimpleValue> cache = new LruCache<String, SimpleValue>(2);

    String key1 = DEFAULT_KEY + 1;
    String key2 = DEFAULT_KEY + 2;
    SimpleValue value = new SimpleValue(true, true);
    cache.put(key1, value);
    cache.put(key2, value);
    assertThat(cache.getCurrentValue(key1)).isEqualTo(value);
    assertThat(cache.getCurrentValue(key2)).isEqualTo(value);
    assertThat(cache.size()).isEqualTo(2);

    cache.clear();
    assertThat(cache.getCurrentValue(key1)).isNull();
    assertThat(cache.getCurrentValue(key2)).isNull();
    assertThat(cache.size()).isZero();
  }
}
