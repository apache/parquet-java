package redelm.io;

import redelm.Log;

public class RecordConsumerWrapper extends RecordConsumer {
    private static final boolean DEBUG = Log.DEBUG;

    private final RecordConsumer delegate;

    int indent = 0;

    public RecordConsumerWrapper(RecordConsumer delegate) {
      this.delegate = delegate;
    }

    @Override
    public void startField(String field) {
      if (DEBUG) logOpen(field);
      delegate.startField(field);
    }

    private void logOpen(String field) {
      indent();
      System.out.println("<"+field+">");
    }

    private void indent() {
      for (int i = 0; i < indent; i++) {
        System.out.print("  ");
      }
    }

    @Override
    public void startGroup() {
      if (DEBUG) ++indent;
      delegate.startGroup();
    }

    @Override
    public void addInt(int value) {
      if (DEBUG) {
        indent();
        log(value);
      }
      delegate.addInt(value);
    }

    @Override
    public void addString(String value) {
      if (DEBUG) {
        indent();
        log(value);
      }
      delegate.addString(value);
    }

    @Override
    public void addBoolean(boolean value) {
      if (DEBUG) {
        indent();
        log(value);
      }
      delegate.addBoolean(value);
    }

    private void log(Object value) {
      System.out.println(value);
    }

    @Override
    public void addBinary(byte[] value) {
      if (DEBUG) {
        indent();
        log(value);
      }
      delegate.addBinary(value);
    }

    @Override
    public void endGroup() {
      if (DEBUG) --indent;
      delegate.endGroup();
    }

    @Override
    public void endField(String field) {
      if (DEBUG) logClose(field);
      delegate.endField(field);
    }

    private void logClose(String field) {
      indent();
      System.out.println("</"+field+">");
    }

}
