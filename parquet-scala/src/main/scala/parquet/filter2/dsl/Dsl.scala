package parquet.filter2.dsl

import parquet.filter2.{Filter, FilterPredicate, UserDefinedPredicate}
import parquet.io.api.Binary

/**
 * Instead of using the methods in [[Filter]] directly in scala code,
 * use this Dsl instead. Example usage:
 *
 * {{{
 * import parquet.filter2.dsl.Dsl._
 *
 * val abc = IntColumn("a.b.c")
 * val xyz = DoubleColumn("x.y.z")
 *
 * val myPredicate = !(abc > 10 && (xyz === 17 || ((xyz !== 13) && (xyz <= 20))))
 *
 * }}}
 *
 * Note that while the operators >, >=, <, <= all work, the == and != operators do not.
 * Using == or != will result in a runtime exception. Instead use === and !==
 *
 * This is due to a limitation in overriding the the equals method.
 */
object Dsl {

  private[Dsl] trait Column[T <: Comparable[T]] {
    val column: parquet.filter2.FilterPredicateOperators.Column[T]

    def ===(v: T) = Filter.eq(column, v)
    def !== (v: T) = Filter.notEq(column, v)
    def >(v: T) = Filter.gt(column, v)
    def >=(v: T) = Filter.gtEq(column, v)
    def <(v: T) = Filter.lt(column, v)
    def <=(v: T) = Filter.ltEq(column, v)
    def filterBy[U <: UserDefinedPredicate[T]](c: Class[U]) = Filter.userDefined(column, c)

    // this is not supported because it allows for easy mistakes. For example:
    // val pred = IntColumn("foo") == "hello"
    // will compile, but pred will be of type boolean instead of FilterPredicate
    override def equals(x: Any) =
      throw new UnsupportedOperationException("You probably meant to use === or !==")

  }

  case class IntColumn(columnPath: String) extends Column[java.lang.Integer] {
    override val column = Filter.intColumn(columnPath)
  }

  case class LongColumn(columnPath: String) extends Column[java.lang.Long] {
    override val column = Filter.longColumn(columnPath)
  }

  case class FloatColumn(columnPath: String) extends Column[java.lang.Float] {
    override val column = Filter.floatColumn(columnPath)
  }

  case class DoubleColumn(columnPath: String) extends Column[java.lang.Double] {
    override val column = Filter.doubleColumn(columnPath)
  }

  case class BooleanColumn(columnPath: String) extends Column[java.lang.Boolean] {
    override val column = Filter.booleanColumn(columnPath)
  }

  case class BinaryColumn(columnPath: String) extends Column[Binary] {
    override val column = Filter.binaryColumn(columnPath)
  }

  implicit def enrich(pred: FilterPredicate): RichPredicate = new RichPredicate(pred)

  class RichPredicate(val pred: FilterPredicate) {
    def &&(other: FilterPredicate) = Filter.and(pred, other)
    def ||(other: FilterPredicate) = Filter.or(pred, other)
    def unary_! = Filter.not(pred)
  }

}
