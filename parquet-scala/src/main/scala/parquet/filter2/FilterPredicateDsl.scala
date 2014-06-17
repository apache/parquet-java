package parquet.filter2

object FilterPredicateDsl {

  case class Column[T](columnPath: String) {
    val column = FilterPredicates.column[T](columnPath)
    def === (v: T) = FilterPredicates.eq(column, v)
    def !== (v: T) = FilterPredicates.notEq(column, v)
    def >(v: T) = FilterPredicates.gt(column, v)
    def >=(v: T) = FilterPredicates.gtEq(column, v)
    def <(v: T) = FilterPredicates.lt(column, v)
    def <=(v: T) = FilterPredicates.ltEq(column, v)

    override def equals(x: Any) =
      throw new UnsupportedOperationException("You probably meant to use === or !==")
  }

  implicit def enrich(pred: FilterPredicate): RichPredicate = new RichPredicate(pred)

  class RichPredicate(val pred: FilterPredicate) {
    def &&(other: FilterPredicate) = FilterPredicates.and(pred, other)
    def ||(other: FilterPredicate) = FilterPredicates.or(pred, other)
    def unary_! = FilterPredicates.not(pred)
  }

}