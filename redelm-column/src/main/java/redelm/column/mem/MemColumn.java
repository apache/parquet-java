package redelm.column.mem;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import redelm.Log;
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;

public abstract class MemColumn implements ColumnReader, ColumnWriter {

  private static final boolean DEBUG = Log.DEBUG;

  private final ColumnDescriptor path;

  private int recordCount;
  private int readRecords;
  protected ByteArrayOutputStream out;
  protected ByteArrayInputStream in;
  private byte[] data;

  private int repetitionLevel;
  private int definitionLevel;
  private boolean valueRead = false;
  private boolean consumed = true;

  public MemColumn(ColumnDescriptor path, int initialSize) {
    this.path = path;
    this.out = new ByteArrayOutputStream(initialSize);
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    write(value);
    ++recordCount;
  }

  private void log(Object value, int r, int d) {
    System.out.println(path+" "+value+" r:"+r+" d:"+d);
  }

  void write(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(String value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    write(value);
    ++recordCount;
  }

  void write(String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    write(value);
    ++recordCount;
  }

  void write(boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    write(value);
    ++recordCount;
  }

  void write(byte[] value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(null, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    ++recordCount;
  }

  private void read() {
    repetitionLevel = in.read();
    definitionLevel = in.read();
    ++readRecords;
    consumed = false;
  }

  public int getCurrentRepetitionLevel() {
    checkRead();
    return repetitionLevel;
  }

  protected void checkRead() {
    if (consumed && !isFullyConsumed()) {
      read();
    }
  }

  protected void checkValueRead() {
    checkRead();
    if (!consumed && !valueRead) {
      readCurrentValue();
      valueRead = true;
    }
  }

  public int getCurrentDefinitionLevel() {
    checkRead();
    return definitionLevel;
  }

  public int size() {
    return recordCount;
  }

  protected abstract void readCurrentValue();

  public void initiateRead() {
    if (data == null) {
      data = out.toByteArray();
      out = null;
    }
    in = new ByteArrayInputStream(data);
    readRecords = 0;
  }

  abstract public String getCurrentValueToString() throws IOException;

  @Override
  public boolean isFullyConsumed() {
    return readRecords >= recordCount;
  }

  @Override
  public void consume() {
    consumed = true;
    valueRead = false;
  }

  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBool() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary() {
    throw new UnsupportedOperationException();
  }

  public int memSize() {
    if (out == null) {
      return data.length;
    } else {
      return out.size();
    }
  }

}
