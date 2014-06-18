package parquet.filter2.dsl

import parquet.filter2.{Filter, FilterPredicate, FilterPredicates}

object Dsl {

  case class Column[T](columnPath: String) {
    val column = Filter.column[T](columnPath)
    def === (v: T) = Filter.eq(column, v)
    def !== (v: T) = Filter.notEq(column, v)
    def >(v: T) = Filter.gt(column, v)
    def >=(v: T) = Filter.gtEq(column, v)
    def <(v: T) = Filter.lt(column, v)
    def <=(v: T) = Filter.ltEq(column, v)

    override def equals(x: Any) =
      throw new UnsupportedOperationException("You probably meant to use === or !==")
  }

  implicit def enrich(pred: FilterPredicate): RichPredicate = new RichPredicate(pred)

  class RichPredicate(val pred: FilterPredicate) {
    def &&(other: FilterPredicate) = Filter.and(pred, other)
    def ||(other: FilterPredicate) = Filter.or(pred, other)
    def unary_! = Filter.not(pred)
  }

}
