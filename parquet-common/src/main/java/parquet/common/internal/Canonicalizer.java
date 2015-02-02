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
package parquet.common.internal;

import java.util.concurrent.ConcurrentHashMap;

/**
 * returns canonical representation of objects (similar to String.intern()) to save memory
 * if a.equals(b) then canonicalize(a) == canonicalize(b)
 * this class is thread safe
 * @author Julien Le Dem
 *
 * @param <T>
 */
public class Canonicalizer<T> {

  private ConcurrentHashMap<T, T> canonicals = new ConcurrentHashMap<T, T>();

  /**
   * @param value the value to canonicalize
   * @return the corresponding canonical value
   */
  final public T canonicalize(T value) {
    T canonical = canonicals.get(value);
    if (canonical == null) {
      value = toCanonical(value);
      T existing = canonicals.putIfAbsent(value, value);
      // putIfAbsent is atomic, making sure we always return the same canonical representation of the value
      if (existing == null) {
        canonical = value;
      } else {
        canonical = existing;
      }
    }
    return canonical;
  }

  /**
   * @param value the value to canonicalize if needed
   * @return the canonicalized value
   */
  protected T toCanonical(T value) {
    return value;
  }
}

