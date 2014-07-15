package parquet.filter2.dsl

import parquet.filter2.predicate.{FilterApi, FilterPredicate, Operators, UserDefinedPredicate}
import parquet.io.api.Binary

/**
 * Instead of using the methods in [[FilterApi]] directly in scala code,
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
    val column: Operators.Column[T]

    def ===(v: T) = FilterApi.eq(column, v)
    def !== (v: T) = FilterApi.notEq(column, v)
    def >(v: T) = FilterApi.gt(column, v)
    def >=(v: T) = FilterApi.gtEq(column, v)
    def <(v: T) = FilterApi.lt(column, v)
    def <=(v: T) = FilterApi.ltEq(column, v)
    def filterBy[U <: UserDefinedPredicate[T]](c: Class[U]) = FilterApi.userDefined(column, c)

    // this is not supported because it allows for easy mistakes. For example:
    // val pred = IntColumn("foo") == "hello"
    // will compile, but pred will be of type boolean instead of FilterPredicate
    override def equals(x: Any) =
      throw new UnsupportedOperationException("You probably meant to use === or !==")

  }

  case class IntColumn(columnPath: String) extends Column[java.lang.Integer] {
    override val column = FilterApi.intColumn(columnPath)
  }

  case class LongColumn(columnPath: String) extends Column[java.lang.Long] {
    override val column = FilterApi.longColumn(columnPath)
  }

  case class FloatColumn(columnPath: String) extends Column[java.lang.Float] {
    override val column = FilterApi.floatColumn(columnPath)
  }

  case class DoubleColumn(columnPath: String) extends Column[java.lang.Double] {
    override val column = FilterApi.doubleColumn(columnPath)
  }

  case class BooleanColumn(columnPath: String) extends Column[java.lang.Boolean] {
    override val column = FilterApi.booleanColumn(columnPath)
  }

  case class BinaryColumn(columnPath: String) extends Column[Binary] {
    override val column = FilterApi.binaryColumn(columnPath)
  }

  implicit def enrich(pred: FilterPredicate): RichPredicate = new RichPredicate(pred)

  class RichPredicate(val pred: FilterPredicate) {
    def &&(other: FilterPredicate) = FilterApi.and(pred, other)
    def ||(other: FilterPredicate) = FilterApi.or(pred, other)
    def unary_! = FilterApi.not(pred)
  }

}
