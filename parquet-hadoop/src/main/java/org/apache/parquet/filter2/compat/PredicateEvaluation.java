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

package org.apache.parquet.filter2.compat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;

/**
 * Used in Filters to mark whether we should DROP the block if data matches the condition.
 * If we cannot decide whether the block matches, it will be always safe to return BLOCK_MIGHT_MATCH.
 * We use Boolean Object here to distinguish the value type, please do not modify it.
 */
public class PredicateEvaluation {
  /* The block might match, but we cannot decide yet, will check in the other filters. */
  public static final Boolean BLOCK_MIGHT_MATCH = new Boolean(false);
  /* The block can match for sure. */
  public static final Boolean BLOCK_MUST_MATCH = new Boolean(false);
  /* The block can't match for sure */
  public static final Boolean BLOCK_CANNOT_MATCH = new Boolean(true);

  public static Boolean evaluateAnd(Operators.And and, FilterPredicate.Visitor<Boolean> predicate) {
    Boolean left = and.getLeft().accept(predicate);
    if (left == BLOCK_CANNOT_MATCH) {
      // seems unintuitive to put an || not an && here but we can
      // drop a chunk of records if we know that either the left or
      // the right predicate agrees that no matter what we don't
      // need this chunk.
      return BLOCK_CANNOT_MATCH;
    }
    Boolean right = and.getRight().accept(predicate);
    if (right == BLOCK_CANNOT_MATCH) {
      return BLOCK_CANNOT_MATCH;
    } else if (left == BLOCK_MUST_MATCH && right == BLOCK_MUST_MATCH) {
      // if left and right operation all must needs the block, then we must take the block
      return BLOCK_MUST_MATCH;
    } else {
      return BLOCK_MIGHT_MATCH;
    }
  }

  public static Boolean evaluateOr(Operators.Or or, FilterPredicate.Visitor<Boolean> predicate) {
    Boolean left = or.getLeft().accept(predicate);
    if (left == BLOCK_MUST_MATCH) {
      // if left or right operation must need the block, then we must take the block
      return BLOCK_MUST_MATCH;
    }
    Boolean right = or.getRight().accept(predicate);
    if (right == BLOCK_MUST_MATCH) {
      // if left or right operation must need the block, then we must take the block
      return BLOCK_MUST_MATCH;
    } else if (left == BLOCK_CANNOT_MATCH && right == BLOCK_CANNOT_MATCH) {
      // seems unintuitive to put an && not an || here
      // but we can only drop a chunk of records if we know that
      // both the left and right predicates agree that no matter what
      // we don't need this chunk.
      return BLOCK_CANNOT_MATCH;
    } else {
      return BLOCK_MIGHT_MATCH;
    }
  }

  /* The predicates which can be verified exactly */
  private static List<Boolean> EXACT_PREDICATES = new ArrayList<>(Arrays.asList(BLOCK_MUST_MATCH, BLOCK_CANNOT_MATCH));

  /**
   * Whether the predicate can be verified exactly in one filter, then the other filters can be skipped for optimization
   */
  public static Boolean isExactPredicate(Boolean predicate) {
    for (Boolean exactPredicate : EXACT_PREDICATES) {
      if (exactPredicate == predicate) {
        return true;
      }
    }
    return false;
  }

  public static void checkPredicate(Boolean predicate) {
    if (predicate != BLOCK_CANNOT_MATCH
      && predicate != BLOCK_MIGHT_MATCH
      && predicate != BLOCK_MUST_MATCH) {
      throw new IllegalArgumentException("predicate should be BLOCK_CANNOT_MATCH, BLOCK_MIGHT_MATCH or BLOCK_MUST_MATCH");
    }
  }


  // Only for Unit Test
  public static void setTestExactPredicate(List<Boolean> predicate) {
    EXACT_PREDICATES = predicate;
  }
}
