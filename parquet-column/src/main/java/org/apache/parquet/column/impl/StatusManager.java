package org.apache.parquet.column.impl;

/**
 * Interface to manage the current error status. It is used to share the status of all the different (column, page,
 * etc.) writer/reader instances.
 */
interface StatusManager {

  /**
   * Creates an instance of the default {@link StatusManager} implementation.
   *
   * @return the newly created {@link StatusManager} instance
   */
  static StatusManager create() {
    return new StatusManager() {
      private boolean aborted;

      @Override
      public void abort() {
        aborted = true;
      }

      @Override
      public boolean isAborted() {
        return aborted;
      }
    };
  }

  /**
   * To be invoked if the current process is to be aborted. For example in case of an exception is occurred during
   * writing a page.
   */
  void abort();

  /**
   * Returns whether the current process is aborted.
   *
   * @return {@code true} if the current process is aborted, {@code false} otherwise
   */
  boolean isAborted();
}
