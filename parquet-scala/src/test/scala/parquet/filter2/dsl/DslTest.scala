package parquet.filter2.dsl

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import parquet.filter2.Filter

@RunWith(classOf[JUnitRunner])
class DslTest extends FlatSpec{
  import Dsl._

  "predicates" should "be correctly constructed using the dsl" in {
    val abc = Column[Int]("a.b.c")
    val xyz = Column[Double]("x.y.z")

    val complexPredicate = !(abc > 10 && (xyz === 17 || ((xyz !== 13) && (xyz <= 20))))
    val abcGt = Filter.gt(abc.column, 10)
    val xyzAnd = Filter.and(Filter.notEq(xyz.column, 13.0), Filter.ltEq(xyz.column, 20.0))
    val xyzEq = Filter.eq(xyz.column, 17.0)
    val xyzPred = Filter.or(xyzEq, xyzAnd)
    val expected = Filter.not(Filter.and(abcGt, xyzPred))

    assert(complexPredicate === expected)
  }

  "Column == and != " should "throw a helpful warning" in {
    val abc = Column[Int]("a.b.c")

    intercept[UnsupportedOperationException] {
      abc == 10
    }

    intercept[UnsupportedOperationException] {
      abc != 10
    }
  }
}
