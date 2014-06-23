package parquet.filter2.dsl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import parquet.filter2.Filter
import parquet.filter2.UserDefinedPredicates.IntUserDefinedPredicate
import parquet.filter2.FilterPredicates.{IntUserDefined, Or}

class DummyFilter extends IntUserDefinedPredicate {
  override def keep(value: Int): Boolean = false

  override def canDrop(min: Int, max: Int, inverted: Boolean): Boolean = false
}

@RunWith(classOf[JUnitRunner])
class DslTest extends FlatSpec{
  import Dsl._

  "predicates" should "be correctly constructed using the dsl" in {
    val abc = IntColumn("a.b.c")
    val xyz = DoubleColumn("x.y.z")

    val complexPredicate = !(abc > 10 && (xyz === 17 || ((xyz !== 13) && (xyz <= 20))))
    val abcGt = Filter.gt[java.lang.Integer](abc.column, 10)
    val xyzAnd = Filter.and(Filter.notEq[java.lang.Double](xyz.column, 13.0), Filter.ltEq[java.lang.Double](xyz.column, 20.0))
    val xyzEq = Filter.eq[java.lang.Double](xyz.column, 17.0)
    val xyzPred = Filter.or(xyzEq, xyzAnd)
    val expected = Filter.not(Filter.and(abcGt, xyzPred))

    assert(complexPredicate === expected)
  }

  "user defined predicates" should "be correctly constructed" in {
    val abc = IntColumn("a.b.c")
    val pred = (abc > 10) || abc.filterBy(classOf[DummyFilter])

    val expected = Filter.or(Filter.gt[java.lang.Integer](abc.column, 10), Filter.intPredicate(abc.column, classOf[DummyFilter]))
    assert(pred === expected)
    val intUserDefined = pred.asInstanceOf[Or].getRight.asInstanceOf[IntUserDefined[DummyFilter]]

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
