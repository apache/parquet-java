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

import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Visitor;

import static parquet.Preconditions.checkNotNull;

/**
 * Determines whether an {@link IncrementallyUpdatedFilterPredicate} is satisfied or not.
 * This implementation makes the assumption that all {@link ValueInspector}s in an unknown state
 * represent columns with a null value, and updates them accordingly.
 *
 * TODO: We could also build an evaluator that detects if enough values are known to determine the outcome
 * TODO: of the predicate and quit the record assembly early. (https://issues.apache.org/jira/browse/PARQUET-37)
 */
public class IncrementallyUpdatedFilterPredicateEvaluator implements Visitor {
  private static final IncrementallyUpdatedFilterPredicateEvaluator INSTANCE = new IncrementallyUpdatedFilterPredicateEvaluator();

  public static boolean evaluate(IncrementallyUpdatedFilterPredicate pred) {
    checkNotNull(pred, "pred");
    return pred.accept(INSTANCE);
  }

  private IncrementallyUpdatedFilterPredicateEvaluator() {}

  @Override
  public boolean visit(ValueInspector p) {
    if (!p.isKnown()) {
      p.updateNull();
    }
    return p.getResult();
  }

  @Override
  public boolean visit(And and) {
    return and.getLeft().accept(this) && and.getRight().accept(this);
  }

  @Override
  public boolean visit(Or or) {
    return or.getLeft().accept(this) || or.getRight().accept(this);
  }
}
