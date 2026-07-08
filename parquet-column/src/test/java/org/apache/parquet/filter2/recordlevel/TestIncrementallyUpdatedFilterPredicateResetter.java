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

import static org.apache.parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.doubleMoreThan10;
import static org.apache.parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.intIsEven;
import static org.apache.parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.intIsNull;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import org.junit.Test;

public class TestIncrementallyUpdatedFilterPredicateResetter {
  @Test
  public void testReset() {

    ValueInspector intIsNull = intIsNull();
    ValueInspector intIsEven = intIsEven();
    ValueInspector doubleMoreThan10 = doubleMoreThan10();

    IncrementallyUpdatedFilterPredicate pred = new Or(intIsNull, new And(intIsEven, doubleMoreThan10));

    intIsNull.updateNull();
    intIsEven.update(11);
    doubleMoreThan10.update(20.0D);

    assertThat(intIsNull.isKnown()).isTrue();
    assertThat(intIsEven.isKnown()).isTrue();
    assertThat(doubleMoreThan10.isKnown()).isTrue();

    IncrementallyUpdatedFilterPredicateResetter.reset(pred);

    assertThat(intIsNull.isKnown()).isFalse();
    assertThat(intIsEven.isKnown()).isFalse();
    assertThat(doubleMoreThan10.isKnown()).isFalse();

    intIsNull.updateNull();
    assertThat(intIsNull.isKnown()).isTrue();
    assertThat(intIsEven.isKnown()).isFalse();
    assertThat(doubleMoreThan10.isKnown()).isFalse();

    IncrementallyUpdatedFilterPredicateResetter.reset(pred);
    assertThat(intIsNull.isKnown()).isFalse();
    assertThat(intIsEven.isKnown()).isFalse();
    assertThat(doubleMoreThan10.isKnown()).isFalse();
  }
}
