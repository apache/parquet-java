package parquet.filter2.dsl

import parquet.filter2.{Filter, FilterPredicate, FilterPredicates}
import parquet.filter2.UserDefinedPredicates._

object Dsl {

  private[Dsl] trait Column[T] {
    val column: parquet.filter2.FilterPredicates.Column[T]

    def === (v: T) = Filter.eq(column, v)
    def !== (v: T) = Filter.notEq(column, v)
    def >(v: T) = Filter.gt(column, v)
    def >=(v: T) = Filter.gtEq(column, v)
    def <(v: T) = Filter.lt(column, v)
    def <=(v: T) = Filter.ltEq(column, v)

    override def equals(x: Any) =
      throw new UnsupportedOperationException("You probably meant to use === or !==")
  }

  case class IntColumn(columnPath: String ) extends Column[java.lang.Integer] {
    override val column = Filter.column[java.lang.Integer](columnPath)
    def filterBy[T <: IntUserDefinedPredicate](c: Class[T]) = Filter.intUserDefined(column, c)
  }

  case class LongColumn(columnPath: String ) extends Column[java.lang.Long] {
    override val column = Filter.column[java.lang.Long](columnPath)
    def filterBy[T <: LongUserDefinedPredicate](c: Class[T]) = Filter.longUserDefined(column, c)
  }

  case class FloatColumn(columnPath: String ) extends Column[java.lang.Float] {
    override val column = Filter.column[java.lang.Float](columnPath)
    def filterBy[T <: FloatUserDefinedPredicate](c: Class[T]) = Filter.floatUserDefined(column, c)
  }

  case class DoubleColumn(columnPath: String ) extends Column[java.lang.Double] {
    override val column = Filter.column[java.lang.Double](columnPath)
    def filterBy[T <: DoubleUserDefinedPredicate](c: Class[T]) = Filter.doubleUserDefined(column, c)
  }

  case class BinaryColumn(columnPath: String ) extends Column[Array[Byte]] {
    override val column = Filter.column[Array[Byte]](columnPath)
    def filterBy[T <: BinaryUserDefinedPredicate](c: Class[T]) = Filter.binaryUserDefined(column, c)
  }

  case class StringColumn(columnPath: String ) extends Column[String] {
    override val column = Filter.column[String](columnPath)
    def filterBy[T <: StringUserDefinedPredicate](c: Class[T]) = Filter.stringUserDefined(column, c)
  }

  implicit def enrich(pred: FilterPredicate): RichPredicate = new RichPredicate(pred)

  class RichPredicate(val pred: FilterPredicate) {
    def &&(other: FilterPredicate) = Filter.and(pred, other)
    def ||(other: FilterPredicate) = Filter.or(pred, other)
    def unary_! = Filter.not(pred)
  }

}
