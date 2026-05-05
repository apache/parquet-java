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

import org.apache.parquet.util.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to assist binding to Hadoop APIs through reflection.
 */
final class BindingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BindingUtils.class);

  private BindingUtils() {}

  /**
   * Get an invocation from the source class, which will be unavailable() if
   * the class is null or the method isn't found.
   *
   * @param <T> return type
   * @param source source. If null, the method is a no-op.
   * @param returnType return type class (unused)
   * @param name method name
   * @param parameterTypes parameters
   *
   * @return the method or "unavailable"
   */
  static <T> DynMethods.UnboundMethod loadInvocation(
      Class<?> source, Class<? extends T> returnType, String name, Class<?>... parameterTypes) {

    if (source != null) {
      final DynMethods.UnboundMethod m = new DynMethods.Builder(name)
          .impl(source, name, parameterTypes)
          .orNoop()
          .build();
      if (m.isNoop()) {
        // this is a sign of a mismatch between this class's expected
        // signatures and actual ones.
        // log at debug.
        LOG.debug("Failed to load method {} from {}", name, source);
      } else {
        LOG.debug("Found method {} from {}", name, source);
      }
      return m;
    } else {
      return noop(name);
    }
  }

  /**
   * Create a no-op method.
   *
   * @param name method name
   *
   * @return a no-op method.
   */
  static DynMethods.UnboundMethod noop(final String name) {
    return new DynMethods.Builder(name).orNoop().build();
  }

  /**
   * Given a sequence of methods, verify that they are all available.
   *
   * @param methods methods
   *
   * @return true if they are all implemented
   */
  static boolean implemented(DynMethods.UnboundMethod... methods) {
    for (DynMethods.UnboundMethod method : methods) {
      if (method.isNoop()) {
        return false;
      }
    }
    return true;
  }
}
