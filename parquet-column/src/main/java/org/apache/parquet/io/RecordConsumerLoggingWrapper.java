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
package org.apache.parquet.io;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * This class can be used to wrap an actual RecordConsumer and log all calls
 *
 * @author Julien Le Dem
 *
 */
public class RecordConsumerLoggingWrapper extends RecordConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordConsumerLoggingWrapper.class);
    private static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();
    private static final boolean WARN_ENABLED = LOGGER.isWarnEnabled();
    private static final boolean INFO_ENABLED = LOGGER.isInfoEnabled();
    private static final boolean ERROR_ENABLED = LOGGER.isErrorEnabled();

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
      if (DEBUG_ENABLED) {
        logOpen(field);
      }
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
      LOGGER.debug(indent() + value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startGroup() {
      if (DEBUG_ENABLED) {
        ++indent;
      }
      if (DEBUG_ENABLED) {
        log("<!-- start group -->");
      }
      delegate.startGroup();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addInteger(int value) {
      if (DEBUG_ENABLED) {
        log(value);
      }
      delegate.addInteger(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addLong(long value) {
      if (DEBUG_ENABLED) {
        log(value);
      }
      delegate.addLong(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBoolean(boolean value) {
      if (DEBUG_ENABLED) {
        log(value);
      }
      delegate.addBoolean(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBinary(Binary value) {
      if (DEBUG_ENABLED) {
        log(Arrays.toString(value.getBytesUnsafe()));
      }
      delegate.addBinary(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addFloat(float value) {
      if (DEBUG_ENABLED) {
        log(value);
      }
      delegate.addFloat(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDouble(double value) {
      if (DEBUG_ENABLED) {
        log(value);
      }
      delegate.addDouble(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() {
      if (DEBUG_ENABLED) {
        log("<!-- flush -->");
      }
      delegate.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endGroup() {
      if (DEBUG_ENABLED) {
        log("<!-- end group -->");
      }
      if (DEBUG_ENABLED) {
        --indent;
      }
      delegate.endGroup();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endField(String field, int index) {
      if (DEBUG_ENABLED) {
        logClose(field);
      }
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
      if (DEBUG_ENABLED) {
        log("<!-- start message -->");
      }
      delegate.startMessage();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endMessage() {
      delegate.endMessage();
      if (DEBUG_ENABLED) {
        log("<!-- end message -->");
      }
    }

}
