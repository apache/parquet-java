/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.io;

import parquet.Log;

/**
 * This class can be used to wrap an actual RecordConsumer and log all calls
 *
 * @author Julien Le Dem
 *
 */
public class RecordConsumerLoggingWrapper extends RecordConsumer {
    private static final Log logger = Log.getLog(RecordConsumerLoggingWrapper.class);
    private static final boolean DEBUG = Log.DEBUG;

    private final RecordConsumer delegate;

    int indent = 0;

    /**
     * all calls a delegate to the wrapped delegate
     * @param delegate
     */
    public RecordConsumerLoggingWrapper(RecordConsumer delegate) {
      this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startField(String field, int index) {
      if (DEBUG) logOpen(field);
      delegate.startField(field, index);
    }

    private void logOpen(String field) {
      log("<"+field+">");
    }

    private String indent() {
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < indent; i++) {
        result.append("  ");
      }
      return result.toString();
    }

    private void log(Object value) {
      logger.debug(indent() + value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startGroup() {
      if (DEBUG) ++indent;
      if (DEBUG) log("<!-- start group -->");
      delegate.startGroup();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addInteger(int value) {
      if (DEBUG) log(value);
      delegate.addInteger(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addLong(long value) {
      if (DEBUG) log(value);
      delegate.addLong(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBoolean(boolean value) {
      if (DEBUG) log(value);
      delegate.addBoolean(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBinary(Binary value) {
      if (DEBUG) log(value.toStringUsingUTF8());
      delegate.addBinary(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addFloat(float value) {
      if (DEBUG) log(value);
      delegate.addFloat(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDouble(double value) {
      if (DEBUG) log(value);
      delegate.addDouble(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endGroup() {
      if (DEBUG) log("<!-- end group -->");
      if (DEBUG) --indent;
      delegate.endGroup();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endField(String field, int index) {
      if (DEBUG) logClose(field);
      delegate.endField(field, index);
    }

    private void logClose(String field) {
      log("</"+field+">");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startMessage() {
      if (DEBUG) log("<!-- start message -->");
      delegate.startMessage();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endMessage() {
      delegate.endMessage();
      if (DEBUG) log("<!-- end message -->");
    }

}
