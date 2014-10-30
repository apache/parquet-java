package parquet.filter2.dsl

import java.io.Serializable
import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}

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

  private[Dsl] trait Column[T <: Comparable[T], C <: Operators.Column[T], S <: java.io.Serializable] {
    val javaColumn: C

    def filterBy[U <: UserDefinedPredicate[T, S]](clazz: Class[U], o: S) = FilterApi.userDefined(javaColumn, clazz, o)

    // this is not supported because it allows for easy mistakes. For example:
    // val pred = IntColumn("foo") == "hello"
    // will compile, but pred will be of type boolean instead of FilterPredicate
    override def equals(x: Any) =
      throw new UnsupportedOperationException("You probably meant to use === or !==")
  }

  case class IntColumn(columnPath: String) extends Column[JInt, Operators.IntColumn, Serializable] {
    override val javaColumn = FilterApi.intColumn(columnPath)
  }

  case class LongColumn(columnPath: String) extends Column[JLong, Operators.LongColumn, Serializable] {
    override val javaColumn = FilterApi.longColumn(columnPath)
  }

  case class FloatColumn(columnPath: String) extends Column[JFloat, Operators.FloatColumn, Serializable] {
    override val javaColumn = FilterApi.floatColumn(columnPath)
  }

  case class DoubleColumn(columnPath: String) extends Column[JDouble, Operators.DoubleColumn, Serializable] {
    override val javaColumn = FilterApi.doubleColumn(columnPath)
  }

  case class BooleanColumn(columnPath: String) extends Column[JBoolean, Operators.BooleanColumn, Serializable] {
    override val javaColumn = FilterApi.booleanColumn(columnPath)
  }

  case class BinaryColumn(columnPath: String) extends Column[Binary, Operators.BinaryColumn, Serializable] {
    override val javaColumn = FilterApi.binaryColumn(columnPath)
  }

  implicit def enrichEqNotEq[T <: Comparable[T], C <: Operators.Column[T] with Operators.SupportsEqNotEq, S <: Serializable](column: Column[T, C, S]): SupportsEqNotEq[T,C, S] = new SupportsEqNotEq(column)

  class SupportsEqNotEq[T <: Comparable[T], C <: Operators.Column[T] with Operators.SupportsEqNotEq, S <: Serializable](val column: Column[T, C, S]) {
    def ===(v: T) = FilterApi.eq(column.javaColumn, v)
    def !== (v: T) = FilterApi.notEq(column.javaColumn, v)
  }

  implicit def enrichLtGt[T <: Comparable[T], C <: Operators.Column[T] with Operators.SupportsLtGt, S <: Serializable](column: Column[T, C, S]): SupportsLtGt[T,C, S] = new SupportsLtGt(column)

  class SupportsLtGt[T <: Comparable[T], C <: Operators.Column[T] with Operators.SupportsLtGt, S <: Serializable](val column: Column[T, C, S]) {
    def >(v: T) = FilterApi.gt(column.javaColumn, v)
    def >=(v: T) = FilterApi.gtEq(column.javaColumn, v)
    def <(v: T) = FilterApi.lt(column.javaColumn, v)
    def <=(v: T) = FilterApi.ltEq(column.javaColumn, v)
  }

  implicit def enrichPredicate(pred: FilterPredicate): RichPredicate = new RichPredicate(pred)
  
  class RichPredicate(val pred: FilterPredicate) {
    def &&(other: FilterPredicate) = FilterApi.and(pred, other)
    def ||(other: FilterPredicate) = FilterApi.or(pred, other)
    def unary_! = FilterApi.not(pred)
  }

  implicit def stringToBinary(s: String): Binary = Binary.fromString(s)

}
