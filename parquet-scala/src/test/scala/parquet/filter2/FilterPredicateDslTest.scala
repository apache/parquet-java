package parquet.filter2

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import parquet.filter2.{FilterPredicates => JPred}

@RunWith(classOf[JUnitRunner])
class FilterPredicateDslTest extends FlatSpec{
  import FilterPredicateDsl._

  "predicates" should "be correctly constructed using the dsl" in {
    val abc = Column[Int]("a.b.c")
    val xyz = Column[Double]("x.y.z")

    val complexPredicate = !(abc > 10 && (xyz === 17 || ((xyz !== 13) && (xyz <= 20))))
    val abcGt = JPred.gt(abc.column, 10)
    val xyzAnd = JPred.and(JPred.notEq(xyz.column, 13.0), JPred.ltEq(xyz.column, 20.0))
    val xyzEq = JPred.eq(xyz.column, 17.0)
    val xyzPred = JPred.or(xyzEq, xyzAnd)
    val expected = JPred.not(JPred.and(abcGt, xyzPred))

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
