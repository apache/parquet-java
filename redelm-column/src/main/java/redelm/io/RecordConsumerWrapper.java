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
package redelm.io;

import redelm.Log;

public class RecordConsumerWrapper extends RecordConsumer {
    private static final Log logger = Log.getLog(RecordConsumerWrapper.class);
    private static final boolean DEBUG = Log.DEBUG;

    private final RecordConsumer delegate;

    int indent = 0;

    public RecordConsumerWrapper(RecordConsumer delegate) {
      this.delegate = delegate;
    }

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

    @Override
    public void startGroup() {
      if (DEBUG) ++indent;
      if (DEBUG) log("<!-- start group -->");
      delegate.startGroup();
    }

    @Override
    public void addInt(int value) {
      if (DEBUG) log(value);
      delegate.addInt(value);
    }

    @Override
    public void addLong(long value) {
      if (DEBUG) log(value);
      delegate.addLong(value);
    }

    @Override
    public void addString(String value) {
      if (DEBUG) log(value);
      delegate.addString(value);
    }

    @Override
    public void addBoolean(boolean value) {
      if (DEBUG) log(value);
      delegate.addBoolean(value);
    }

    @Override
    public void addBinary(byte[] value) {
      if (DEBUG) log(value);
      delegate.addBinary(value);
    }

    @Override
    public void addFloat(float value) {
      if (DEBUG) log(value);
      delegate.addFloat(value);
    }

    @Override
    public void addDouble(double value) {
      if (DEBUG) log(value);
      delegate.addDouble(value);
    }
    @Override
    public void endGroup() {
      if (DEBUG) log("<!-- end group -->");
      if (DEBUG) --indent;
      delegate.endGroup();
    }

    @Override
    public void endField(String field, int index) {
      if (DEBUG) logClose(field);
      delegate.endField(field, index);
    }

    private void logClose(String field) {
      log("</"+field+">");
    }

    @Override
    public void startMessage() {
      if (DEBUG) log("<!-- start message -->");
      delegate.startMessage();
    }

    @Override
    public void endMessage() {
      delegate.endMessage();
      if (DEBUG) log("<!-- end message -->");
    }

}
