package parquet.filter2.dsl

import java.lang.{Double => JDouble, Integer => JInt}
import java.io.Serializable;

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import parquet.filter2.predicate.Operators.{Or, UserDefined, DoubleColumn => JDoubleColumn, IntColumn => JIntColumn}
import parquet.filter2.predicate.{FilterApi, Statistics, UserDefinedPredicate}

class DummyFilter extends UserDefinedPredicate[JInt, java.io.Serializable] {
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
    val pred = (abc > 10) || abc.filterBy(classOf[DummyFilter], null)

    val expected = FilterApi.or(FilterApi.gt[JInt, JIntColumn](abc.javaColumn, 10), FilterApi.userDefined[JInt, DummyFilter, java.io.Serializable](abc.javaColumn, classOf[DummyFilter], null))
    assert(pred === expected)
    val intUserDefined = pred.asInstanceOf[Or].getRight.asInstanceOf[UserDefined[JInt, DummyFilter, java.io.Serializable]]

    assert(intUserDefined.getUserDefinedPredicateClass === classOf[DummyFilter])
    assert(intUserDefined.getUserDefinedPredicate.isInstanceOf[DummyFilter])
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
