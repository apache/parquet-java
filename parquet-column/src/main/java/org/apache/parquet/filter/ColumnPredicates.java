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
package org.apache.parquet.filter;

import java.util.Objects;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;

/**
 * ColumnPredicates class provides checks for column values. Factory methods
 * are provided for standard predicates which wrap the job of getting the
 * correct value from the column.
 */
public class ColumnPredicates {

  public interface Predicate {
    boolean apply(ColumnReader input);
  }

  public interface PredicateFunction<T> {
    boolean functionToApply(T input);
  }

  /* provide the following to avoid boxing primitives */

  public interface IntegerPredicateFunction {
    boolean functionToApply(int input);
  }

  public interface LongPredicateFunction {
    boolean functionToApply(long input);
  }

  public interface FloatPredicateFunction {
    boolean functionToApply(float input);
  }

  public interface DoublePredicateFunction {
    boolean functionToApply(double input);
  }

  public interface BooleanPredicateFunction {
    boolean functionToApply(boolean input);
  }

  public static Predicate equalTo(final String target) {
    Objects.requireNonNull(target, "target cannot be null");
    return input -> target.equals(input.getBinary().toStringUsingUTF8());
  }

  public static Predicate applyFunctionToString(final PredicateFunction<String> fn) {
    return input -> fn.functionToApply(input.getBinary().toStringUsingUTF8());
  }

  public static Predicate equalTo(final int target) {
    return input -> input.getInteger() == target;
  }

  public static Predicate applyFunctionToInteger(final IntegerPredicateFunction fn) {
    return input -> fn.functionToApply(input.getInteger());
  }

  public static Predicate equalTo(final long target) {
    return input -> input.getLong() == target;
  }

  public static Predicate applyFunctionToLong(final LongPredicateFunction fn) {
    return input -> fn.functionToApply(input.getLong());
  }

  public static Predicate equalTo(final float target) {
    return input -> input.getFloat() == target;
  }

  public static Predicate applyFunctionToFloat(final FloatPredicateFunction fn) {
    return input -> fn.functionToApply(input.getFloat());
  }

  public static Predicate equalTo(final double target) {
    return input -> input.getDouble() == target;
  }

  public static Predicate applyFunctionToDouble(final DoublePredicateFunction fn) {
    return input -> fn.functionToApply(input.getDouble());
  }

  public static Predicate equalTo(final boolean target) {
    return input -> input.getBoolean() == target;
  }

  public static Predicate applyFunctionToBoolean(final BooleanPredicateFunction fn) {
    return input -> fn.functionToApply(input.getBoolean());
  }

  public static <E extends Enum> Predicate equalTo(final E target) {
    Objects.requireNonNull(target, "target cannot be null");
    final String targetAsString = target.name();
    return input -> targetAsString.equals(input.getBinary().toStringUsingUTF8());
  }

  public static Predicate applyFunctionToBinary(final PredicateFunction<Binary> fn) {
    return input -> fn.functionToApply(input.getBinary());
  }
}
