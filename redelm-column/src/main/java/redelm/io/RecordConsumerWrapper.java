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

    @Override
    public void startGroup() {
      if (DEBUG) ++indent;
      if (DEBUG) log("<!-- start group -->");
      delegate.startGroup();
    }

    @Override
    public void addInt(int value) {
      if (DEBUG) {
        log(value);
      }
      delegate.addInt(value);
    }

    @Override
    public void addString(String value) {
      if (DEBUG) {
        log(value);
      }
      delegate.addString(value);
    }

    @Override
    public void addBoolean(boolean value) {
      if (DEBUG) {
        log(value);
      }
      delegate.addBoolean(value);
    }

    private void log(Object value) {
      logger.debug(indent() + value);
    }

    @Override
    public void addBinary(byte[] value) {
      if (DEBUG) {
        log(value);
      }
      delegate.addBinary(value);
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
