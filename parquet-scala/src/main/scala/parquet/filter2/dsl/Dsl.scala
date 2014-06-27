package parquet.filter2.dsl

import parquet.filter2.UserDefinedPredicates._
import parquet.filter2.{Filter, FilterPredicate}
import parquet.io.api.Binary

object Dsl {

  private[Dsl] trait Column[T] {
    val column: parquet.filter2.FilterPredicateOperators.Column[T]

    def ===(v: T) = Filter.eq(column, v)
    def !== (v: T) = Filter.notEq(column, v)
    def >(v: T) = Filter.gt(column, v)
    def >=(v: T) = Filter.gtEq(column, v)
    def <(v: T) = Filter.lt(column, v)
    def <=(v: T) = Filter.ltEq(column, v)

    override def equals(x: Any) =
      throw new UnsupportedOperationException("You probably meant to use === or !==")

  }

  case class IntColumn(columnPath: String) extends Column[java.lang.Integer] {
    override val column = Filter.intColumn(columnPath)
    def filterBy[T <: IntUserDefinedPredicate](c: Class[T]) = Filter.intPredicate(column, c)
  }

  case class LongColumn(columnPath: String) extends Column[java.lang.Long] {
    override val column = Filter.longColumn(columnPath)
    def filterBy[T <: LongUserDefinedPredicate](c: Class[T]) = Filter.longPredicate(column, c)
  }

  case class FloatColumn(columnPath: String) extends Column[java.lang.Float] {
    override val column = Filter.floatColumn(columnPath)
    def filterBy[T <: FloatUserDefinedPredicate](c: Class[T]) = Filter.floatPredicate(column, c)
  }

  case class DoubleColumn(columnPath: String) extends Column[java.lang.Double] {
    override val column = Filter.doubleColumn(columnPath)
    def filterBy[T <: DoubleUserDefinedPredicate](c: Class[T]) = Filter.doublePredicate(column, c)
  }

  case class BooleanColumn(columnPath: String) extends Column[java.lang.Boolean] {
    override val column = Filter.booleanColumn(columnPath)
  }

  case class BinaryColumn(columnPath: String) extends Column[Binary] {
    override val column = Filter.binaryColumn(columnPath)
    def filterBy[T <: BinaryUserDefinedPredicate](c: Class[T]) = Filter.binaryPredicate(column, c)
  }

  case class StringColumn(columnPath: String) extends Column[String] {
    override val column = Filter.stringColumn(columnPath)
    def filterBy[T <: StringUserDefinedPredicate](c: Class[T]) = Filter.stringPredicate(column, c)
  }

  implicit def enrich(pred: FilterPredicate): RichPredicate = new RichPredicate(pred)

  class RichPredicate(val pred: FilterPredicate) {
    def &&(other: FilterPredicate) = Filter.and(pred, other)
    def ||(other: FilterPredicate) = Filter.or(pred, other)
    def unary_! = Filter.not(pred)
  }

}
