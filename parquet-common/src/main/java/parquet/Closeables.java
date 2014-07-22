package parquet;

import java.io.Closeable;
import java.io.IOException;

/**
 * Utility for working with {@link java.io.Closeable}ss
 */
public final class Closeables {
  private Closeables() { }

  private static final Log LOG = Log.getLog(Closeables.class);

  /**
   * Closes a (potentially null) closeable.
   * @param c can be null
   * @throws IOException if c.close() throws an IOException.
   */
  public static void close(Closeable c) throws IOException {
    if (c == null) { return; }
    c.close();
  }

  /**
   * Closes a (potentially null) closeable, swallowing any IOExceptions thrown by
   * c.close(). The exception will be logged.
   * @param c can be null
   */
  public static void closeAndSwallowIOExceptions(Closeable c) {
    if (c == null) { return; }
    try {
      c.close();
    } catch (IOException e) {
      LOG.warn("Encountered exception closing closeable", e);
    }
  }
}
