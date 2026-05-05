/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.hadoop.util.wrapped.io;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods to work with futures, based on.
 * {@code org.apache.hadoop.util.functional.FutureIO}.
 *
 * These methods are used in production code.
 */
public final class FutureIO {

  private static final Logger LOG = LoggerFactory.getLogger(FutureIO.class);

  /**
   * Given a future, evaluate it.
   * <p>
   * Any exception generated in the future is
   * extracted and rethrown.
   * </p>
   *
   * @param future future to evaluate
   * @param timeout timeout to wait
   * @param unit time unit.
   * @param <T> type of the result.
   *
   * @return the result, if all went well.
   *
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   * @throws TimeoutException the future timed out.
   */
  public static <T> T awaitFuture(final Future<T> future, final long timeout, final TimeUnit unit)
      throws InterruptedIOException, IOException, RuntimeException, TimeoutException {
    try {
      LOG.debug("Awaiting future");
      return future.get(timeout, unit);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString()).initCause(e);
    } catch (ExecutionException | CompletionException e) {
      throw unwrapInnerException(e);
    }
  }

  /**
   * Given a future, evaluate it.
   * <p>
   * Any exception generated in the future is
   * extracted and rethrown.
   * </p>
   * @param future future to evaluate
   * @param <T> type of the result.
   * @return the result, if all went well.
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   */
  public static <T> T awaitFuture(final Future<T> future)
      throws InterruptedIOException, IOException, RuntimeException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString()).initCause(e);
    } catch (ExecutionException e) {
      throw unwrapInnerException(e);
    }
  }
  /**
   * From the inner cause of an execution exception, extract the inner cause
   * to an IOException, raising Errors immediately.
   * <ol>
   *   <li> If it is an IOE: Return.</li>
   *   <li> If it is a {@link UncheckedIOException}: return the cause</li>
   *   <li> Completion/Execution Exceptions: extract and repeat</li>
   *   <li> If it is an RTE or Error: throw.</li>
   *   <li> Any other type: wrap in an IOE</li>
   *   <li> If there is no cause: wrap the passed exception by an IOE</li>
   * </ol>
   *
   * Recursively handles wrapped Execution and Completion Exceptions in
   * case something very complicated has happened.
   *
   * @param e exception.
   *
   * @return an IOException extracted or built from the cause.
   *
   * @throws RuntimeException if that is the inner cause.
   * @throws Error if that is the inner cause.
   */
  @SuppressWarnings("ChainOfInstanceofChecks")
  public static IOException unwrapInnerException(final Throwable e) {
    Throwable cause = e.getCause();
    if (cause instanceof IOException) {
      return (IOException) cause;
    } else if (cause instanceof UncheckedIOException) {
      // this is always an IOException
      return ((UncheckedIOException) cause).getCause();
    } else if (cause instanceof CompletionException || cause instanceof ExecutionException) {
      return unwrapInnerException(cause);
    } else if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else if (cause instanceof Error) {
      throw (Error) cause;
    } else if (cause != null) {
      // other type: wrap with a new IOE
      return new IOException(cause);
    } else {
      // this only happens if there was no cause.
      return new IOException(e);
    }
  }
}
