package parquet.filter2;

/**
 * See {@link parquet.filter2.RecordPredicateBuilder}
 * TODO(alexlevenson): This is a very stateful interface, and hooks into
 * TODO(alexlevenson): the wrong layer in record assembly. We should refactor how
 * TODO(alexlevenson): record filters work.
 */
public interface RecordPredicate {
  boolean isMatch();
}
