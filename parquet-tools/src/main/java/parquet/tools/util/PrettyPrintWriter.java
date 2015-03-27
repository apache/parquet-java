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
package parquet.tools.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import parquet.tools.Main;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

public class PrettyPrintWriter extends PrintWriter {
  public static final String MORE = " [more]...";
  public static final String LINE_SEP = System.getProperty("line.separator");
  public static final Span DEFAULT_APPEND;
  public static final char DEFAULT_COLUMN_SEP = ':';
  public static final int DEFAULT_MAX_COLUMNS = 1;
  public static final int DEFAULT_COLUMN_PADDING = 1;
  public static final int DEFAULT_TABS = 4;
  public static final int DEFAULT_WIDTH;
  public static final int DEFAULT_COLORS;

  private static final String RESET = "\u001B[0m";

  public static final String MODE_OFF         = "0";
  public static final String MODE_BOLD        = "1";
  public static final String MODE_UNDER       = "4";
  public static final String MODE_BLINK       = "5";
  public static final String MODE_REVERSE     = "7";
  public static final String MODE_CONCEALED   = "8";

  public static final String FG_COLOR_BLACK   = "30";
  public static final String FG_COLOR_RED     = "31";
  public static final String FG_COLOR_GREEN   = "32";
  public static final String FG_COLOR_YELLOW  = "33";
  public static final String FG_COLOR_BLUE    = "34";
  public static final String FG_COLOR_MAGENTA = "35";
  public static final String FG_COLOR_CYAN    = "36";
  public static final String FG_COLOR_WHITE   = "37";

  public static final String BG_COLOR_BLACK   = "40";
  public static final String BG_COLOR_RED     = "41";
  public static final String BG_COLOR_GREEN   = "42";
  public static final String BG_COLOR_YELLOW  = "43";
  public static final String BG_COLOR_BLUE    = "44";
  public static final String BG_COLOR_MAGENTA = "45";
  public static final String BG_COLOR_CYAN    = "46";
  public static final String BG_COLOR_WHITE   = "47";

  public enum WhiteSpaceHandler { ELIMINATE_NEWLINES, COLLAPSE_WHITESPACE }

  static {
    int consoleWidth = 80;
    int numColors = 0;

    String columns = System.getenv("COLUMNS");
    if (columns != null && !columns.isEmpty()) {
      try {
        consoleWidth = Integer.parseInt(columns);
      } catch (Throwable th) {
      }
    }

    String colors = System.getenv("COLORS");
    if (colors != null && !colors.isEmpty()) {
      try {
        numColors = Integer.parseInt(colors);
        if (numColors < 0) numColors = 0;
      } catch (Throwable th) {
      }
    }

    String termout = System.getenv("TERMOUT");
    if (termout != null && !termout.isEmpty()) {
      if (!"y".equalsIgnoreCase(termout) &&
          !"yes".equalsIgnoreCase(termout) &&
          !"t".equalsIgnoreCase(termout) &&
          !"true".equalsIgnoreCase(termout) &&
          !"on".equalsIgnoreCase(termout)) {
        consoleWidth = Integer.MAX_VALUE;
        numColors = 0;
      }
    }

    if (System.getProperty("DISABLE_COLORS", null) != null) {
      numColors = 0;
    }

    DEFAULT_WIDTH = consoleWidth;
    DEFAULT_COLORS = numColors;

    if (numColors > 0) {
      DEFAULT_APPEND = mkspan(MORE, null, FG_COLOR_RED, null);
    } else {
      DEFAULT_APPEND = mkspan(MORE);
    }
  }

  private final StringBuilder formatString;
  private final Formatter formatter;
  private final ArrayList<Line> buffer;

  private final boolean autoColumn;
  private final boolean autoCrop;
  private final Span appendToLongLine;
  private final int consoleWidth;
  private final int tabWidth;
  private final char columnSeparator;
  private final int maxColumns;
  private final int columnPadding;
  private final long maxBufferedLines;
  private final boolean flushOnTab;
  private final WhiteSpaceHandler whiteSpaceHandler;
  private int tabLevel;
  private String colorMode;
  private String colorForeground;
  private String colorBackground;
  private String tabs;

  private PrettyPrintWriter(OutputStream out, boolean autoFlush,
    boolean autoColumn, boolean autoCrop, Span appendToLongLine,
    int consoleWidth, int tabWidth, char columnSeparator,
    int maxColumns, int columnPadding, long maxBufferedLines, 
    boolean flushOnTab, WhiteSpaceHandler whiteSpaceHandler) {
    super(out, autoFlush && !autoColumn);
    this.autoColumn = autoColumn;
    this.autoCrop = autoCrop;
    this.appendToLongLine = appendToLongLine;
    this.consoleWidth = consoleWidth;
    this.tabWidth = tabWidth;
    this.columnSeparator = columnSeparator;
    this.maxColumns = maxColumns;
    this.maxBufferedLines = maxBufferedLines;
    this.columnPadding = columnPadding;
    this.flushOnTab = flushOnTab;
    this.whiteSpaceHandler = whiteSpaceHandler;

    this.buffer = new ArrayList<Line>();
    this.formatString = new StringBuilder();
    this.formatter = new Formatter(this.formatString);

    this.colorMode = null;
    this.colorForeground = null;
    this.colorBackground = null;

    this.tabLevel = 0;
    this.tabs = "";

    this.buffer.add(new Line());
  }

  public void setTabLevel(int level) {
    this.tabLevel = level;
    this.tabs = Strings.repeat(" ", tabWidth * level);
    if (flushOnTab) flushColumns();
  }

  public void incrementTabLevel() {
    setTabLevel(tabLevel + 1);
  }

  public void decrementTabLevel() {
    if (tabLevel == 0) {
      return;
    }

    setTabLevel(tabLevel - 1);
  }

  private int determineNumColumns() {
    int max = 0;
    for (Line line : buffer) {
      int num = line.countCharacter(columnSeparator);
      if (num > max) {
        max = num;
      }
    }

    return max > maxColumns ? maxColumns : max;
  }

  private int[] determineColumnWidths() {
    int columns = determineNumColumns();
    if (columns == 0) {
      return null;
    }

    int[] widths = new int[columns];
    for (Line line : buffer) {
      for (int last = 0, idx = 0; last < line.length() && idx < columns; ++idx) {
        int pos = line.indexOf(columnSeparator, last);
        if (pos < 0) break;

        int wid = pos - last + 1 + columnPadding;
        if (wid > widths[idx]) {
          widths[idx] = wid;
        }

        last = line.firstNonWhiteSpace(idx + 1);
      }
    }

    return widths;
  }

  private Line toColumns(int[] widths, Line line) throws IOException {
    int last = 0;
    for (int i = 0; i < widths.length; ++i) {
      int width = widths[i];

      int idx = line.indexOf(columnSeparator, last);
      if (idx < 0) break;

      if ((idx+1) <= width) {
        line.spaceOut(width - (idx+1), idx+1);
      }

      last = line.firstNonWhiteSpace(idx + 1);
    }

    return line;
  }

  public void flushColumns() {
      flushColumns(false);
  }

  private void flushColumns(boolean preserveLast) {
    int size = buffer.size();

    int[] widths = null;
    if (autoColumn) {
      widths = determineColumnWidths();
    }

    StringBuilder builder = new StringBuilder();

    try {
      for (int i = 0; i < size - 1; ++i) {
        Line line = buffer.get(i);
        if (widths != null) {
          line = toColumns(widths, line);
        }

        fixupLine(line);
        builder.setLength(0);
        line.toString(builder);

        super.out.append(builder.toString());
        super.out.append(LINE_SEP);
      }

      if (!preserveLast) {
        Line line = buffer.get(size - 1);
        if (widths != null) {
          line = toColumns(widths, line);
        }

        fixupLine(line);
        builder.setLength(0);
        line.toString(builder);
        super.out.append(builder.toString());
      }

      super.out.flush();
    } catch (IOException ex) {
    }

    Line addback = null;
    if (preserveLast) {
        addback = buffer.get(size - 1);
    }

    buffer.clear();
    if (addback != null) buffer.add(addback);
    else buffer.add(new Line());
  }

  private void flushIfNeeded() {
    flushIfNeeded(false);
  }

  private void flushIfNeeded(boolean preserveLast) {
    if (!autoColumn || buffer.size() > maxBufferedLines) {
      flushColumns(preserveLast);
    }
  }

  private void appendToCurrent(String s) {
    int size = buffer.size();
    Line value = buffer.get(size - 1);
    if (value.isEmpty()) {
      value.append(tabs());
    }

    value.append(span(s));
  }

  private void fixupLine(Line line) {
    if (autoCrop) {
      line.trimTo(consoleWidth, appendToLongLine);
    }
  }

  private void print(String s, boolean mayHaveNewlines) {
    if (s == null) {
      appendToCurrent("null");
      return;
    }

    if (s.isEmpty()) {
      return;
    }

    if (LINE_SEP.equals(s)) {
      buffer.add(new Line());
      flushIfNeeded();
      return;
    }

    if (whiteSpaceHandler != null) {
      boolean endswith = s.endsWith(LINE_SEP);
      switch (whiteSpaceHandler) {
        case ELIMINATE_NEWLINES:
          s = s.replaceAll("\\r\\n|\\r|\\n", " ");
          break;

        case COLLAPSE_WHITESPACE:
          s = s.replaceAll("\\s+", " ");
          break;
      }

      mayHaveNewlines = endswith;
      if (endswith) s = s + LINE_SEP;
    }

    if (!mayHaveNewlines) {
      appendToCurrent(s);
      return;
    }

    String lines[] = s.split("\\r?\\n", -1);
    appendToCurrent(lines[0]);

    for (int i = 1; i < lines.length; ++i) {
      String value = lines[i];
      if (value.isEmpty()) {
        buffer.add(new Line());
      } else {
        Line line = new Line();
        line.append(tabs());
        line.append(span(value, true));
        buffer.add(line);
      }
    }

    resetColor();
    flushIfNeeded(true);
  }

  @Override
  public void print(String s) {
    print(s, true);
  }

  @Override
  public void println() {
    print(LINE_SEP, true);
    flushIfNeeded();
  }

  @Override
  public void println(String x) {
    print(x);
    println();
  }

  @Override
  public void print(boolean b) {
    print(String.valueOf(b), false);
  }

  @Override
  public void print(char c) {
    print(String.valueOf(c), false);
  }

  @Override
  public void print(int i) {
    print(String.valueOf(i), false);
  }

  @Override
  public void print(long l) {
    print(String.valueOf(l), false);
  }

  @Override
  public void print(float f) {
    print(String.valueOf(f), false);
  }

  @Override
  public void print(double d) {
    print(String.valueOf(d), false);
  }

  @Override
  public void print(char[] s) {
    print(String.valueOf(s), true);
  }

  @Override
  public void print(Object obj) {
    print(String.valueOf(obj), true);
  }

  @Override
  public PrintWriter printf(String format, Object... args) {
    return printf(formatter.locale(), format, args);
  }

  @Override
  public PrintWriter printf(Locale l, String format, Object... args) {
    formatter.format(l, format, args);

    String results = formatString.toString();
    formatString.setLength(0);

    print(results);
    flushIfNeeded();
    return this;
  }

  @Override
  public PrintWriter format(String format, Object... args) {
    return printf(format, args);
  }

  @Override
  public PrintWriter format(Locale l, String format, Object... args) {
    return printf(l, format, args);
  }

  @Override
  public PrintWriter append(char c) {
    print(c);
    return this;
  }

  @Override
  public PrintWriter append(CharSequence csq) {
    if (csq == null) { print("null"); return this; }
    return append(csq, 0, csq.length());
  }

  @Override
  public PrintWriter append(CharSequence csq, int start, int end) {
    if (csq == null) { print("null"); return this; }
    print(csq.subSequence(start,end).toString());
    return this;
  }

  @Override
  public void println(boolean x) {
    print(x);
    println();
  }

  @Override
  public void println(char x) {
    print(x);
    println();
  }

  @Override
  public void println(int x) {
    print(x);
    println();
  }

  @Override
  public void println(long x) {
    print(x);
    println();
  }

  @Override
  public void println(float x) {
    print(x);
    println();
  }

  @Override
  public void println(double x) {
    print(x);
    println();
  }

  @Override
  public void println(char[] x) {
    print(x);
    println();
  }

  @Override
  public void println(Object x) {
    print(x);
    println();
  }

  public void rule(char c) {
    if (tabs.length() >= consoleWidth) return;

    int width = consoleWidth;
    if (width == Integer.MAX_VALUE) {
      width = 100;
    }

    println(Strings.repeat(String.valueOf(c), width - tabs.length()));
  }

  public boolean acceptColorModification = true;
  public PrettyPrintWriter iff(boolean predicate) {
    if (!predicate && acceptColorModification) {
      resetColor();
    } else {
      acceptColorModification = false;
    }

    return this;
  }

  public PrettyPrintWriter otherwise() {
    acceptColorModification = false;
    return this;
  }

  public PrettyPrintWriter black() {
    if (!acceptColorModification) return this;
    colorForeground = FG_COLOR_BLACK;
    return this;
  }

  public PrettyPrintWriter red() {
    if (!acceptColorModification) return this;
    colorForeground = FG_COLOR_RED;
    return this;
  }

  public PrettyPrintWriter green() {
    if (!acceptColorModification) return this;
    colorForeground = FG_COLOR_GREEN;
    return this;
  }

  public PrettyPrintWriter yellow() {
    if (!acceptColorModification) return this;
    colorForeground = FG_COLOR_YELLOW;
    return this;
  }

  public PrettyPrintWriter blue() {
    if (!acceptColorModification) return this;
    colorForeground = FG_COLOR_BLUE;
    return this;
  }

  public PrettyPrintWriter magenta() {
    if (!acceptColorModification) return this;
    colorForeground = FG_COLOR_MAGENTA;
    return this;
  }

  public PrettyPrintWriter cyan() {
    if (!acceptColorModification) return this;
    colorForeground = FG_COLOR_CYAN;
    return this;
  }

  public PrettyPrintWriter white() {
    if (!acceptColorModification) return this;
    colorForeground = FG_COLOR_WHITE;
    return this;
  }

  public PrettyPrintWriter bgblack() {
    if (!acceptColorModification) return this;
    colorBackground = BG_COLOR_BLACK;
    return this;
  }

  public PrettyPrintWriter bgred() {
    if (!acceptColorModification) return this;
    colorBackground = BG_COLOR_RED;
    return this;
  }

  public PrettyPrintWriter bggreen() {
    if (!acceptColorModification) return this;
    colorBackground = BG_COLOR_GREEN;
    return this;
  }

  public PrettyPrintWriter bgyellow() {
    if (!acceptColorModification) return this;
    colorBackground = BG_COLOR_YELLOW;
    return this;
  }

  public PrettyPrintWriter bgblue() {
    if (!acceptColorModification) return this;
    colorBackground = BG_COLOR_BLUE;
    return this;
  }

  public PrettyPrintWriter bgmagenta() {
    if (!acceptColorModification) return this;
    colorBackground = BG_COLOR_MAGENTA;
    return this;
  }

  public PrettyPrintWriter bgcyan() {
    if (!acceptColorModification) return this;
    colorBackground = BG_COLOR_CYAN;
    return this;
  }

  public PrettyPrintWriter bgwhite() {
    if (!acceptColorModification) return this;
    colorBackground = BG_COLOR_WHITE;
    return this;
  }


  public PrettyPrintWriter bold() {
    if (!acceptColorModification) return this;
    colorMode = MODE_BOLD;
    return this;
  }

  public PrettyPrintWriter blink() {
    if (!acceptColorModification) return this;
    colorMode = MODE_BLINK;
    return this;
  }

  public PrettyPrintWriter concealed() {
    if (!acceptColorModification) return this;
    colorMode = MODE_CONCEALED;
    return this;
  }

  public PrettyPrintWriter off() {
    if (!acceptColorModification) return this;
    colorMode = MODE_OFF;
    return this;
  }

  public PrettyPrintWriter underscore() {
    if (!acceptColorModification) return this;
    colorMode = MODE_UNDER;
    return this;
  }

  public PrettyPrintWriter reverse() {
    if (!acceptColorModification) return this;
    colorMode = MODE_REVERSE;
    return this;
  }

  public static Builder stdoutPrettyPrinter() {
    return new Builder(Main.out).withAutoFlush();
  }

  public static Builder stderrPrettyPrinter() {
    return new Builder(Main.err).withAutoFlush();
  }

  public static Builder newPrettyPrinter(OutputStream out) {
    return new Builder(out);
  }

  public static final class Builder {
    private final OutputStream out;
    private boolean autoFlush;

    private boolean autoColumn;
    private char columnSeparator;
    private int maxColumns;
    private int columnPadding;
    private long maxBufferedLines;

    private boolean autoCrop;
    private int consoleWidth;
    private Span appendToLongLine;

    private int tabWidth;
    private boolean flushOnTab;
    private WhiteSpaceHandler whiteSpaceHandler;

    public Builder(OutputStream out) {
      this.out = out;
      this.autoFlush = false;
      this.autoColumn = false;
      this.flushOnTab = false;
      this.columnSeparator = DEFAULT_COLUMN_SEP;
      this.maxColumns = DEFAULT_MAX_COLUMNS;
      this.columnPadding = DEFAULT_COLUMN_PADDING;
      this.autoCrop = false;
      this.consoleWidth = DEFAULT_WIDTH;
      this.appendToLongLine = null;
      this.tabWidth = DEFAULT_TABS;
      this.whiteSpaceHandler = null;
      this.maxBufferedLines = Long.MAX_VALUE;
    }

    public Builder withAutoFlush() {
      this.autoFlush = true;
      return this;
    }

    public Builder withAutoCrop() {
      return withAutoCrop(DEFAULT_WIDTH);
    }

    public Builder withAutoCrop(int consoleWidth) {
      return withAutoCrop(consoleWidth, DEFAULT_APPEND);
    }

    public Builder withAutoCrop(int consoleWidth, String appendToLong) {
      return withAutoCrop(consoleWidth, mkspan(appendToLong));
    }

    public Builder withAutoCrop(int consoleWidth, Span appendToLong) {
      this.consoleWidth = consoleWidth;
      this.appendToLongLine = appendToLong;
      this.autoCrop = true;
      return this;
    }

    public Builder withTabSize(int tabWidth) {
      this.tabWidth = tabWidth;
      return this;
    }

    public Builder withAutoColumn() {
      return withAutoColumn(DEFAULT_COLUMN_SEP);
    }

    public Builder withAutoColumn(char columnSeparator) {
      return withAutoColumn(columnSeparator, DEFAULT_MAX_COLUMNS);
    }

    public Builder withAutoColumn(char columnSeparator, int maxColumns) {
      this.autoColumn = true;
      this.columnSeparator = columnSeparator;
      this.maxColumns = maxColumns;
      return this;
    }

    public Builder withColumnPadding(int columnPadding) {
      this.columnPadding = columnPadding;
      return this;
    }

    public Builder withWhitespaceHandler(WhiteSpaceHandler whiteSpaceHandler) {
      this.whiteSpaceHandler = whiteSpaceHandler;
      return this;
    }

    public Builder withMaxBufferedLines(long maxBufferedLines) {
        this.maxBufferedLines = maxBufferedLines;
        return this;
    }

    public Builder withFlushOnTab() {
        this.flushOnTab = true;
        return this;
    }

    public PrettyPrintWriter build() {
      return new PrettyPrintWriter(out, autoFlush, autoColumn, autoCrop, appendToLongLine, consoleWidth, tabWidth, columnSeparator, maxColumns, columnPadding, maxBufferedLines, flushOnTab, whiteSpaceHandler);
    }
  }

  private Span tabs() {
    return new Span(tabs);
  }

  private Span span(String span) {
    return span(span, false);
  }

  private void resetColor() {
    acceptColorModification = true;

    colorMode = null;
    colorForeground = null;
    colorBackground = null;
  }

  public static Span mkspan(String span) {
      return new Span(span);
  }

  public static Span mkspan(String span, String color) {
    return mkspan(span, null, color, null);
  }

  public static Span mkspan(String span, String colorMode, String colorForeground, String colorBackground) {
    if (DEFAULT_COLORS > 0 && (colorMode != null || colorForeground != null || colorBackground != null)) {
      String color = "\u001B[" + Joiner.on(';').skipNulls().join(colorMode, colorForeground, colorBackground) + "m";
      return new Span(span, color);
    } else {
      return mkspan(span);
    }
  }

  private Span span(String span, boolean keepColor) {
    Span result;
    if (DEFAULT_COLORS > 0 && (colorMode != null || colorForeground != null || colorBackground != null)) {
      result = mkspan(span, colorMode, colorForeground, colorBackground);
    } else {
      result = mkspan(span);
    }

    if (!keepColor) {
      resetColor();
    }

    return result;
  }

  public static final class Line {
    private List<Span> spans;
    private int length;

    public Line() {
      this.spans = new ArrayList<Span>();
    }

    public void append(Span span) {
      length += span.length();
      if (spans.isEmpty()) {
        spans.add(span);
        return;
      }
      
      Span last = spans.get(spans.size() - 1);
      if (last.canAppend(span)) {
        last.append(span);
      } else {
        spans.add(span);
      }
    }

    public boolean isEmpty() {
      return length == 0;
    }

    public int length() {
      return length;
    }

    public int indexOf(char ch, int start) {
      int offset = 0;
      for (Span span : spans) {
        if (start > span.length()) {
          start -= span.length();
          continue;
        }

        int idx = span.indexOf(ch, start);
        if (idx >= 0) return offset + idx;

        offset += span.length() - start;
        start = 0;
      }

      return -1;
    }

    public void spaceOut(int width, int start) {
      for (Span span : spans) {
        if (start > span.length()) {
          start -= span.length();
          continue;
        }

        span.spaceOut(width, start);
        return;
      }
    }

    public int firstNonWhiteSpace(int start) {
      return start;
    }

    public int countCharacter(char ch) {
      int result = 0;
      for (Span span : spans) {
        result += span.countCharacter(ch);
      }

      return result;
    }

    public void trimTo(int width, Span appendToLongLine) {
      int i = 0;
      int remaining = width;
      for (i = 0; i < spans.size(); ++i) {
        Span next = spans.get(i);
        if (next.length() > remaining) {
          ++i;
          next.trimTo(remaining, appendToLongLine);
          break;
        }

        remaining -= next.length();
      }

      for (; i < spans.size(); ++i) {
        spans.remove(i);
      }
    }

    public void toString(StringBuilder builder) {
      for (Span span : spans) {
        span.toString(builder);
      }
    }
  }

  public static final class Span {
    private String span;
    private final String color;

    public Span(String span) {
      this(span, null);
    }

    public Span(String span, String color) {
      this.span = span;
      this.color = color;
    }

    public int length() {
      return span.length();
    }

    public boolean isEmpty() {
      return span.isEmpty();
    }

    public int indexOf(char ch, int start) {
      return span.indexOf(ch, start);
    }

    public void spaceOut(int width, int start) {
      int removeTo = start;
      while (removeTo < span.length() && Character.isWhitespace(span.charAt(removeTo))) {
        removeTo++;
      }

      span = span.substring(0,start) + Strings.repeat(" ", width) + span.substring(removeTo);
    }

    public int countCharacter(char ch) {
      int result = 0;
      for (int i = 0; i < span.length(); ++i) {
        if (span.charAt(i) == ch) {
          result++;
        }
      }

      return result;
    }

    public void trimTo(int width, Span appendToLongLine) {
      if (appendToLongLine != null && !appendToLongLine.isEmpty()) {
        int shortten = appendToLongLine.length();
        if (shortten > width) shortten = width;

        span = span.substring(0, width - shortten) + appendToLongLine;
      } else {
        span = span.substring(0, width+1);
      }
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      toString(builder);
      return builder.toString();
    }

    public void toString(StringBuilder builder) {
      if (color != null) builder.append(color);
      builder.append(span);
      if (color != null) builder.append(RESET);
    }

    public void append(Span other) {
      span = span + other.span;
    }

    public boolean canAppend(Span other) {
      if (color == null && other == null) return true;
      if (color == null && other != null) return false;
      return color.equals(other);
    }
  }
}
