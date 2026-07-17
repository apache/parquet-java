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
package org.apache.parquet.filter2.recordlevel;

import static org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicateEvaluator.evaluate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import org.junit.Test;

public class TestIncrementallyUpdatedFilterPredicateEvaluator {

  public static class ShortCircuitException extends RuntimeException {
    public ShortCircuitException() {
      super("this was supposed to short circuit and never get here!");
    }
  }

  public static ValueInspector intIsNull() {
    return new ValueInspector() {
      @Override
      public void updateNull() {
        setResult(true);
      }

      @Override
      public void update(int value) {
        setResult(false);
      }
    };
  }

  public static ValueInspector intIsEven() {
    return new ValueInspector() {
      @Override
      public void updateNull() {
        setResult(false);
      }

      @Override
      public void update(int value) {
        setResult(value % 2 == 0);
      }
    };
  }

  public static ValueInspector doubleMoreThan10() {
    return new ValueInspector() {
      @Override
      public void updateNull() {
        setResult(false);
      }

      @Override
      public void update(double value) {
        setResult(value > 10.0);
      }
    };
  }

  @Test
  public void testValueInspector() {
    // known, and set to false criteria, null considered false
    ValueInspector v = intIsEven();
    v.update(11);
    assertThat(evaluate(v)).isFalse();
    v.reset();

    // known and set to true criteria, null considered false
    v.update(12);
    assertThat(evaluate(v)).isTrue();
    v.reset();

    // known and set to null, null considered false
    v.updateNull();
    assertThat(evaluate(v)).isFalse();
    v.reset();

    // known, and set to false criteria, null considered true
    ValueInspector intIsNull = intIsNull();
    intIsNull.update(10);
    assertThat(evaluate(intIsNull)).isFalse();
    intIsNull.reset();

    // known, and set to false criteria, null considered true
    intIsNull.updateNull();
    assertThat(evaluate(intIsNull)).isTrue();
    intIsNull.reset();

    // unknown, null considered false
    v.reset();
    assertThat(evaluate(v)).isFalse();

    // unknown, null considered true
    intIsNull.reset();
    assertThat(evaluate(intIsNull)).isTrue();
  }

  private void doOrTest(ValueInspector v1, ValueInspector v2, int v1Value, int v2Value, boolean expected) {
    v1.update(v1Value);
    v2.update(v2Value);
    IncrementallyUpdatedFilterPredicate or = new Or(v1, v2);
    assertThat(evaluate(or)).isEqualTo(expected);
    v1.reset();
    v2.reset();
  }

  private void doAndTest(ValueInspector v1, ValueInspector v2, int v1Value, int v2Value, boolean expected) {
    v1.update(v1Value);
    v2.update(v2Value);
    IncrementallyUpdatedFilterPredicate and = new And(v1, v2);
    assertThat(evaluate(and)).isEqualTo(expected);
    v1.reset();
    v2.reset();
  }

  @Test
  public void testOr() {
    ValueInspector v1 = intIsEven();
    ValueInspector v2 = intIsEven();

    int F = 11;
    int T = 12;

    // F || F == F
    doOrTest(v1, v2, F, F, false);
    // F || T == T
    doOrTest(v1, v2, F, T, true);
    // T || F == T
    doOrTest(v1, v2, T, F, true);
    // T || T == T
    doOrTest(v1, v2, T, T, true);
  }

  @Test
  public void testAnd() {
    ValueInspector v1 = intIsEven();
    ValueInspector v2 = intIsEven();

    int F = 11;
    int T = 12;

    // F && F == F
    doAndTest(v1, v2, F, F, false);
    // F && T == F
    doAndTest(v1, v2, F, T, false);
    // T && F == F
    doAndTest(v1, v2, T, F, false);
    // T && T == T
    doAndTest(v1, v2, T, T, true);
  }

  @Test
  public void testShortCircuit() {
    ValueInspector neverCalled = new ValueInspector() {
      @Override
      public boolean accept(Visitor visitor) {
        throw new ShortCircuitException();
      }
    };

    assertThatThrownBy(() -> evaluate(neverCalled))
        .isInstanceOf(ShortCircuitException.class)
        .hasMessage("this was supposed to short circuit and never get here!");

    // T || X should evaluate to true without inspecting X
    ValueInspector v = intIsEven();
    v.update(10);
    IncrementallyUpdatedFilterPredicate or = new Or(v, neverCalled);
    assertThat(evaluate(or)).isTrue();
    v.reset();

    // F && X should evaluate to false without inspecting X
    v.update(11);
    IncrementallyUpdatedFilterPredicate and = new And(v, neverCalled);
    assertThat(evaluate(and)).isFalse();
    v.reset();
  }
}
