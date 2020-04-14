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

import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Visitor;

import java.util.Objects;

/**
 * Resets all the {@link ValueInspector}s in a {@link IncrementallyUpdatedFilterPredicate}.
 */
public final class IncrementallyUpdatedFilterPredicateResetter implements Visitor {
  private static final IncrementallyUpdatedFilterPredicateResetter INSTANCE = new IncrementallyUpdatedFilterPredicateResetter();

  public static void reset(IncrementallyUpdatedFilterPredicate pred) {
    Objects.requireNonNull(pred, "pred cannot be null").accept(INSTANCE);
  }

  private IncrementallyUpdatedFilterPredicateResetter() { }

  @Override
  public boolean visit(ValueInspector p) {
    p.reset();
    return false;
  }

  @Override
  public boolean visit(And and) {
    and.getLeft().accept(this);
    and.getRight().accept(this);
    return false;
  }

  @Override
  public boolean visit(Or or) {
    or.getLeft().accept(this);
    or.getRight().accept(this);
    return false;
  }
}
