package parquet.filter2.dsl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import parquet.filter2.predicate.{UserDefinedPredicate, Operators, FilterApi}
import Operators.{UserDefined, Or}
import FilterApi

class DummyFilter extends UserDefinedPredicate[java.lang.Integer] {
  override def keep(value: java.lang.Integer): Boolean = false

  override def canDrop(min: java.lang.Integer, max: java.lang.Integer): Boolean = false

  override def inverseCanDrop(min: java.lang.Integer, max: java.lang.Integer): Boolean = false
}

@RunWith(classOf[JUnitRunner])
class DslTest extends FlatSpec{
  import parquet.filter2.dsl.Dsl._

  "predicates" should "be correctly constructed using the dsl" in {
    val abc = IntColumn("a.b.c")
    val xyz = DoubleColumn("x.y.z")

    val complexPredicate = !(abc > 10 && (xyz === 17 || ((xyz !== 13) && (xyz <= 20))))
    val abcGt = FilterApi.gt[java.lang.Integer](abc.column, 10)
    val xyzAnd = FilterApi.and(FilterApi.notEq[java.lang.Double](xyz.column, 13.0), FilterApi.ltEq[java.lang.Double](xyz.column, 20.0))
    val xyzEq = FilterApi.eq[java.lang.Double](xyz.column, 17.0)
    val xyzPred = FilterApi.or(xyzEq, xyzAnd)
    val expected = FilterApi.not(FilterApi.and(abcGt, xyzPred))

    assert(complexPredicate === expected)
  }

  "user defined predicates" should "be correctly constructed" in {
    val abc = IntColumn("a.b.c")
    val pred = (abc > 10) || abc.filterBy(classOf[DummyFilter])

    val expected = FilterApi.or(FilterApi.gt[java.lang.Integer](abc.column, 10), FilterApi.userDefined(abc.column, classOf[DummyFilter]))
    assert(pred === expected)
    val intUserDefined = pred.asInstanceOf[Or].getRight.asInstanceOf[UserDefined[java.lang.Integer, DummyFilter]]

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
