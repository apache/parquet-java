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
package parquet.filter2.recordlevel;


import org.junit.Test;

import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.doubleMoreThan10;
import static parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.intIsEven;
import static parquet.filter2.recordlevel.TestIncrementallyUpdatedFilterPredicateEvaluator.intIsNull;

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

    assertTrue(intIsNull.isKnown());
    assertTrue(intIsEven.isKnown());
    assertTrue(doubleMoreThan10.isKnown());

    IncrementallyUpdatedFilterPredicateResetter.reset(pred);

    assertFalse(intIsNull.isKnown());
    assertFalse(intIsEven.isKnown());
    assertFalse(doubleMoreThan10.isKnown());

    intIsNull.updateNull();
    assertTrue(intIsNull.isKnown());
    assertFalse(intIsEven.isKnown());
    assertFalse(doubleMoreThan10.isKnown());

    IncrementallyUpdatedFilterPredicateResetter.reset(pred);
    assertFalse(intIsNull.isKnown());
    assertFalse(intIsEven.isKnown());
    assertFalse(doubleMoreThan10.isKnown());

  }
}
