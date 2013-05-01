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
package parquet;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

/**
 * Simple wrapper around java.util.logging
 * Adds compile time log level.
 * The compiler removes completely if statements that reference to a false constant
 *
 * <code>
 *   if (DEBUG) LOG.debug("removed by the compiler if DEBUG is a false constant")
 * </code>
 *
 * @author Julien Le Dem
 *
 */
public class Log {

  /**
   * this is the compile time log level
   */
  public static final Level LEVEL = Level.INFO;

  public static final boolean DEBUG = (LEVEL.intValue() <= Level.FINE.intValue());
  public static final boolean INFO = (LEVEL.intValue() <= Level.INFO.intValue());
  public static final boolean WARN = (LEVEL.intValue() <= Level.WARNING.intValue());
  public static final boolean ERROR = (LEVEL.intValue() <= Level.SEVERE.intValue());

  static {
    // add a default handler in case there is none
    Logger logger = Logger.getLogger(Log.class.getPackage().getName());
    Handler[] handlers = logger.getHandlers();
    if (handlers == null || handlers.length == 0) {
      logger.setUseParentHandlers(false);
      StreamHandler handler = new StreamHandler(System.out, new Formatter() {
        Date dat = new Date();
        private final static String format = "{0,date} {0,time}";
        private MessageFormat formatter = new MessageFormat(format);

        private Object args[] = new Object[1];

        /**
         * Format the given LogRecord.
         * @param record the log record to be formatted.
         * @return a formatted log record
         */
        public synchronized String format(LogRecord record) {
          StringBuffer sb = new StringBuffer();
          // Minimize memory allocations here.
          dat.setTime(record.getMillis());
          args[0] = dat;
          formatter.format(args, sb, null);
          sb.append(" ");
          sb.append(record.getLevel().getLocalizedName());
          sb.append(": ");
          sb.append(record.getLoggerName());

          sb.append(": ");
          sb.append(formatMessage(record));
          sb.append("\n");
          if (record.getThrown() != null) {
            try {
              StringWriter sw = new StringWriter();
              PrintWriter pw = new PrintWriter(sw);
              record.getThrown().printStackTrace(pw);
              pw.close();
              sb.append(sw.toString());
            } catch (Exception ex) {
            }
          }
          return sb.toString();
        }
      });
      handler.setLevel(LEVEL);
      logger.addHandler(handler);
    }
    logger.setLevel(LEVEL);
  }

  /**
   *
   * @param c the current class
   * @return the corresponding logger
   */
  public static Log getLog(Class<?> c) {
    return new Log(c);
  }

  private Logger logger;

  public Log(Class<?> c) {
    this.logger = Logger.getLogger(c.getName());
  }

  /**
   * prints a debug message
   * @param m
   */
  public void debug(Object m) {
    if (m instanceof Throwable) {
      logger.log(Level.FINE, "", (Throwable)m);
    } else {
      logger.fine(String.valueOf(m));
    }
  }

  /**
   * prints a debug message
   * @param m
   * @param t
   */
  public void debug(Object m, Throwable t) {
    logger.log(Level.FINE, String.valueOf(m), t);
  }

  /**
   * prints an info message
   * @param m
   */
  public void info(Object m) {
    if (m instanceof Throwable) {
      logger.log(Level.INFO, "", (Throwable)m);
    } else {
      logger.info(String.valueOf(m));
    }
  }

  /**
   * prints an info message
   * @param m
   * @param t
   */
  public void info(Object m, Throwable t) {
    logger.log(Level.INFO, String.valueOf(m), t);
  }

  /**
   * prints a warn message
   * @param m
   */
  public void warn(Object m) {
    if (m instanceof Throwable) {
      logger.log(Level.WARNING, "", (Throwable)m);
    } else {
      logger.warning(String.valueOf(m));
    }
  }

  /**
   * prints a warn message
   * @param m
   * @param t
   */
  public void warn(Object m, Throwable t) {
    logger.log(Level.WARNING, String.valueOf(m), t);
  }

  /**
   * prints an error message
   * @param m
   */
  public void error(Object m) {
    if (m instanceof Throwable) {
      logger.log(Level.SEVERE, "", (Throwable)m);
    } else {
      logger.warning(String.valueOf(m));
    }
  }

  /**
   * prints an error message
   * @param m
   * @param t
   */
  public void error(Object m, Throwable t) {
    logger.log(Level.SEVERE, String.valueOf(m), t);
  }

}
