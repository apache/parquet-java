package parquet.hadoop;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestLruCache {
  private static final String DEFAULT_KEY = "test";

  private static final class SimpleEntry implements LruCache.Entry<SimpleEntry> {
    private boolean current;
    private boolean newerThan;

    public SimpleEntry(boolean current, boolean newerThan) {
      this.current = current;
      this.newerThan = newerThan;
    }

    @Override
    public boolean isCurrent() {
      return current;
    }

    public void setCurrent(boolean current) {
      this.current = current;
    }

    @Override
    public boolean isNewerThan(SimpleEntry otherEntry) {
      return newerThan;
    }

  }

  @Test
  public void testMaxSize() {
    LruCache<String, SimpleEntry> cache = new LruCache<String, SimpleEntry>(1);

    String oldKey = DEFAULT_KEY;
    String newKey = oldKey + "_new";

    SimpleEntry oldEntry = new SimpleEntry(true, true);
    cache.put(oldKey, oldEntry);
    assertEquals(oldEntry, cache.getCurrentEntry(oldKey));
    assertEquals(1, cache.size());

    SimpleEntry newEntry = new SimpleEntry(true, true);
    cache.put(newKey, newEntry);
    assertNull(cache.getCurrentEntry(oldKey));
    assertEquals(newEntry, cache.getCurrentEntry(newKey));
    assertEquals(1, cache.size());
  }

  @Test
  public void testOlderEntryIsIgnored() {
    LruCache<String, SimpleEntry> cache = new LruCache<String, SimpleEntry>(1);

    SimpleEntry currentEntry = new SimpleEntry(true, true);
    SimpleEntry notAsCurrentEntry = new SimpleEntry(true, false);
    cache.put(DEFAULT_KEY, currentEntry);
    cache.put(DEFAULT_KEY, notAsCurrentEntry);
    assertEquals(
            "The existing entry in the cache was overwritten",
            currentEntry,
            cache.getCurrentEntry(DEFAULT_KEY)
    );
  }

  @Test
  public void testOutdatedEntryIsIgnored() {
    LruCache<String, SimpleEntry> cache = new LruCache<String, SimpleEntry>(1);

    SimpleEntry outdatedEntry = new SimpleEntry(false, true);
    cache.put(DEFAULT_KEY, outdatedEntry);
    assertEquals(0, cache.size());
    assertNull(cache.getCurrentEntry(DEFAULT_KEY));
  }

  @Test
  public void testCurrentEntryOverwritesExisting() {
    LruCache<String, SimpleEntry> cache = new LruCache<String, SimpleEntry>(1);

    SimpleEntry currentEntry = new SimpleEntry(true, true);
    SimpleEntry notAsCurrentEntry = new SimpleEntry(true, false);
    cache.put(DEFAULT_KEY, notAsCurrentEntry);
    assertEquals(1, cache.size());
    cache.put(DEFAULT_KEY, currentEntry);
    assertEquals(1, cache.size());
    assertEquals(
            "The existing entry in the cache was NOT overwritten",
            currentEntry,
            cache.getCurrentEntry(DEFAULT_KEY)
    );
  }

  @Test
  public void testGetOutdatedEntryReturnsNull() {
    LruCache<String, SimpleEntry> cache = new LruCache<String, SimpleEntry>(1);

    SimpleEntry entry = new SimpleEntry(true, true);
    cache.put(DEFAULT_KEY, entry);
    assertEquals(1, cache.size());
    assertEquals(entry, cache.getCurrentEntry(DEFAULT_KEY));

    entry.setCurrent(false);
    assertNull("The entry should not be current anymore", cache.getCurrentEntry(DEFAULT_KEY));
    assertEquals(0, cache.size());
  }

  @Test
  public void testRemove() {
    LruCache<String, SimpleEntry> cache = new LruCache<String, SimpleEntry>(1);

    SimpleEntry entry = new SimpleEntry(true, true);
    cache.put(DEFAULT_KEY, entry);
    assertEquals(1, cache.size());
    assertEquals(entry, cache.getCurrentEntry(DEFAULT_KEY));

    // remove the only entry
    assertEquals(entry, cache.remove(DEFAULT_KEY));
    assertNull(cache.getCurrentEntry(DEFAULT_KEY));
    assertEquals(0, cache.size());
  }

  @Test
  public void testClear() {
    LruCache<String, SimpleEntry> cache = new LruCache<String, SimpleEntry>(2);

    String key1 = DEFAULT_KEY + 1;
    String key2 = DEFAULT_KEY + 2;
    SimpleEntry entry = new SimpleEntry(true, true);
    cache.put(key1, entry);
    cache.put(key2, entry);
    assertEquals(entry, cache.getCurrentEntry(key1));
    assertEquals(entry, cache.getCurrentEntry(key2));
    assertEquals(2, cache.size());

    cache.clear();
    assertNull(cache.getCurrentEntry(key1));
    assertNull(cache.getCurrentEntry(key2));
    assertEquals(0, cache.size());
  }

}
