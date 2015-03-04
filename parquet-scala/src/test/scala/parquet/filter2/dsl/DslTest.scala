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
package parquet.filter2.dsl

import java.lang.{Double => JDouble, Integer => JInt}
import java.io.Serializable

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import parquet.filter2.predicate.Operators.{Or, UserDefined, UserDefinedByClass, DoubleColumn => JDoubleColumn, IntColumn => JIntColumn}
import parquet.filter2.predicate.{FilterApi, Statistics, UserDefinedPredicate}

class DummyFilter extends UserDefinedPredicate[JInt] with Serializable {
  override def keep(value: JInt): Boolean = false

  override def canDrop(statistics: Statistics[JInt]): Boolean = false

  override def inverseCanDrop(statistics: Statistics[JInt]): Boolean = false
}

@RunWith(classOf[JUnitRunner])
class DslTest extends FlatSpec{
  import parquet.filter2.dsl.Dsl._

  "predicates" should "be correctly constructed using the dsl" in {
    val abc = IntColumn("a.b.c")
    val xyz = DoubleColumn("x.y.z")

    val complexPredicate = !(abc > 10 && (xyz === 17 || ((xyz !== 13) && (xyz <= 20))))
    val abcGt = FilterApi.gt[JInt, JIntColumn](abc.javaColumn, 10)
    val xyzAnd = FilterApi.and(FilterApi.notEq[JDouble, JDoubleColumn](xyz.javaColumn, 13.0),
      FilterApi.ltEq[JDouble, JDoubleColumn](xyz.javaColumn, 20.0))
    val xyzEq = FilterApi.eq[JDouble, JDoubleColumn](xyz.javaColumn, 17.0)
    val xyzPred = FilterApi.or(xyzEq, xyzAnd)
    val expected = FilterApi.not(FilterApi.and(abcGt, xyzPred))

    assert(complexPredicate === expected)
  }

  "user defined predicates" should "be correctly constructed" in {
    val abc = IntColumn("a.b.c")
    val predByClass = (abc > 10) || abc.filterBy(classOf[DummyFilter])
    val instance = new DummyFilter
    val predByInstance = (abc > 10) || abc.filterBy(instance)

    val expectedByClass = FilterApi.or(FilterApi.gt[JInt, JIntColumn](abc.javaColumn, 10), FilterApi.userDefined(abc.javaColumn, classOf[DummyFilter]))
    val expectedByInstance = FilterApi.or(FilterApi.gt[JInt, JIntColumn](abc.javaColumn, 10), FilterApi.userDefined(abc.javaColumn, instance))
    assert(predByClass === expectedByClass)
    assert(predByInstance === expectedByInstance)
  
    val intUserDefinedByClass = predByClass.asInstanceOf[Or].getRight.asInstanceOf[UserDefinedByClass[JInt, DummyFilter]]
    assert(intUserDefinedByClass.getUserDefinedPredicateClass === classOf[DummyFilter])
    assert(intUserDefinedByClass.getUserDefinedPredicate.isInstanceOf[DummyFilter])
    
    val intUserDefinedByInstance = predByInstance.asInstanceOf[Or].getRight.asInstanceOf[UserDefined[JInt, DummyFilter]]
    assert(intUserDefinedByInstance.getUserDefinedPredicate === instance)
  }

  "Column == and != " should "throw a helpful warning" in {
    val abc = IntColumn("a.b.c")

    intercept[UnsupportedOperationException] {
      abc == 10
    }

    intercept[UnsupportedOperationException] {
      abc != 10
    }
  }
}
