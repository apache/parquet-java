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

import static org.apache.parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.intIsEven;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import org.junit.jupiter.api.Test;

public class TestValueInspector {

  @Test
  public void testLifeCycle() {
    ValueInspector v = intIsEven();

    // begins in unknown state
    assertThat(v.isKnown()).isFalse();
    // calling getResult in unknown state throws
    assertThatThrownBy(() -> v.getResult())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("getResult() called on a ValueInspector whose result is not yet known!");

    // update state to known
    v.update(10);

    // v was updated with value 10, so result is known and should be true
    assertThat(v.isKnown()).isTrue();
    assertThat(v.getResult()).isTrue();

    // calling update w/o resetting should throw
    assertThatThrownBy(() -> v.update(11))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("setResult() called on a ValueInspector whose result is already known!"
            + " Did you forget to call reset()?");

    // back to unknown state
    v.reset();

    assertThat(v.isKnown()).isFalse();
    // calling getResult in unknown state throws
    assertThatThrownBy(() -> v.getResult())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("getResult() called on a ValueInspector whose result is not yet known!");

    // v was updated with value 11, so result is known and should be false
    v.update(11);
    assertThat(v.isKnown()).isTrue();
    assertThat(v.getResult()).isFalse();
  }

  @Test
  public void testReusable() {
    List<Integer> values = List.of(2, 4, 7, 3, 8, 8, 11, 200);
    ValueInspector v = intIsEven();

    for (Integer x : values) {
      v.update(x);
      assertThat(v.getResult()).isEqualTo(x % 2 == 0);
      v.reset();
    }
  }
}
