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
package org.apache.parquet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.logging.Level;

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
 * @deprecated This class will be removed in 2.0; use slf4j instead.
 */
@Deprecated
public class Log {

  /**
   * this is the compile time log level
   */
  public static final Level LEVEL = Level.INFO; // should be INFO unless for debugging

  public static final boolean DEBUG = (LEVEL.intValue() <= Level.FINE.intValue());
  public static final boolean INFO = (LEVEL.intValue() <= Level.INFO.intValue());
  public static final boolean WARN = (LEVEL.intValue() <= Level.WARNING.intValue());
  public static final boolean ERROR = (LEVEL.intValue() <= Level.SEVERE.intValue());

  /**
   *
   * @param c the current class
   * @return the corresponding logger
   * @deprecated will be removed in 2.0.0; use org.slf4j.LoggerFactory instead.
   */
  public static Log getLog(Class<?> c) {
    return new Log(c);
  }

  private Logger logger;

  public Log(Class<?> c) {
    this.logger = LoggerFactory.getLogger(c);
  }

  /**
   * prints a debug message
   * @param m
   */
  public void debug(Object m) {
    if (m instanceof Throwable) {
      logger.debug("", (Throwable) m);
    } else {
      logger.debug(String.valueOf(m));
    }
  }

  /**
   * prints a debug message
   * @param m
   * @param t
   */
  public void debug(Object m, Throwable t) {
    logger.debug(String.valueOf(m), t);
  }

  /**
   * prints an info message
   * @param m
   */
  public void info(Object m) {
    if (m instanceof Throwable) {
      logger.info("", (Throwable) m);
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
    logger.info(String.valueOf(m), t);
  }

  /**
   * prints a warn message
   * @param m
   */
  public void warn(Object m) {
    if (m instanceof Throwable) {
      logger.warn("", (Throwable) m);
    } else {
      logger.warn(String.valueOf(m));
    }
  }

  /**
   * prints a warn message
   * @param m
   * @param t
   */
  public void warn(Object m, Throwable t) {
    logger.warn(String.valueOf(m), t);
  }

  /**
   * prints an error message
   * @param m
   */
  public void error(Object m) {
    if (m instanceof Throwable) {
      logger.error("", (Throwable) m);
    } else {
      logger.error(String.valueOf(m));
    }
  }

  /**
   * prints an error message
   * @param m
   * @param t
   */
  public void error(Object m, Throwable t) {
    logger.error(String.valueOf(m), t);
  }

}
