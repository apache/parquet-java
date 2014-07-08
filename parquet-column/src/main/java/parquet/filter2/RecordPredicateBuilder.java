package parquet.filter2;

import java.util.HashMap;
import java.util.Map;

import parquet.Preconditions;
import parquet.column.ColumnReader;
import parquet.filter2.FilterPredicate.Visitor;
import parquet.filter2.FilterPredicateOperators.And;
import parquet.filter2.FilterPredicateOperators.Column;
import parquet.filter2.FilterPredicateOperators.Eq;
import parquet.filter2.FilterPredicateOperators.Gt;
import parquet.filter2.FilterPredicateOperators.GtEq;
import parquet.filter2.FilterPredicateOperators.LogicalNotUserDefined;
import parquet.filter2.FilterPredicateOperators.Lt;
import parquet.filter2.FilterPredicateOperators.LtEq;
import parquet.filter2.FilterPredicateOperators.Not;
import parquet.filter2.FilterPredicateOperators.NotEq;
import parquet.filter2.FilterPredicateOperators.Or;
import parquet.filter2.FilterPredicateOperators.UserDefined;
import parquet.filter2.GenericColumnReader.BinaryColumnReader;
import parquet.filter2.GenericColumnReader.BooleanColumnReader;
import parquet.filter2.GenericColumnReader.DoubleColumnReader;
import parquet.filter2.GenericColumnReader.FloatColumnReader;
import parquet.filter2.GenericColumnReader.IntColumnReader;
import parquet.filter2.GenericColumnReader.LongColumnReader;
import parquet.io.api.Binary;
import parquet.schema.ColumnPathUtil;

/**
 * Builds a {@link RecordPredicate} bound to the given {@link ColumnReader}s, based on the provided
 * {@link FilterPredicate}
 *
 * Note: the supplied predicate must not contain any instances of the not() operator as this is not
 * supported by this filter.
 *
 * the supplied predicate should first be run through {@link parquet.filter2.CollapseLogicalNots} to rewrite it
 * in a form that doesn't make use of the not() operator.
 *
 * the supplied predicate should also have already been run through
 * {@link parquet.filter2.FilterPredicateTypeValidator}
 * to make sure it is compatible with the schema of this file.
 *
 * TODO(alexlevenson): This is currently done by combining a series of anonymous classes, but is a good candidate
 * TODO(alexlevenson): for runtime bytecode generation.
 *
 * TODO(alexlevenson): Need to take nulls into account by inspecting the definition levels
 */
public class RecordPredicateBuilder implements Visitor<RecordPredicate> {

  public static RecordPredicate build(FilterPredicate filterPredicate, Iterable<ColumnReader> columns) {
    return filterPredicate.accept(new RecordPredicateBuilder(columns));
  }

  private final Map<String, ColumnReader> columns;

  private RecordPredicateBuilder(Iterable<ColumnReader> columnReaders) {
    columns = new HashMap<String, ColumnReader>();
    for (ColumnReader reader : columnReaders) {
      String path = ColumnPathUtil.toDotSeparatedString(reader.getDescriptor().getPath());
      columns.put(path, reader);
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> GenericColumnReader<T> getColumnReader(Column<T> column) {
    ColumnReader rawReader = columns.get(column.getColumnPath());
    Preconditions.checkNotNull(rawReader, "Encountered unknown column " + column.getColumnPath());
    Class<T> clazz = column.getColumnType();

    if (clazz.equals(Integer.class)) {
      return (GenericColumnReader) new IntColumnReader(rawReader);
    }

    if (clazz.equals(Long.class)) {
      return (GenericColumnReader) new LongColumnReader(rawReader);
    }

    if (clazz.equals(Float.class)) {
      return (GenericColumnReader) new FloatColumnReader(rawReader);
    }

    if (clazz.equals(Double.class)) {
      return (GenericColumnReader) new DoubleColumnReader(rawReader);
    }

    if (clazz.equals(Boolean.class)) {
      return (GenericColumnReader) new BooleanColumnReader(rawReader);
    }

    if (clazz.equals(Binary.class)) {
      return (GenericColumnReader) new BinaryColumnReader(rawReader);
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
  }

  @Override
  public <T extends Comparable<T>> RecordPredicate visit(Eq<T> eq) {
    final GenericColumnReader<T> reader = getColumnReader(eq.getColumn());
    final T value = eq.getValue();

    if (value == null) {
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.isCurrentValueNull();
        }
      };
    }

    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return reader.getValue().compareTo(value) == 0;
      }
    };

  }

  @Override
  public <T extends Comparable<T>> RecordPredicate visit(NotEq<T> notEq) {
    final GenericColumnReader<T> reader = getColumnReader(notEq.getColumn());
    final T value = notEq.getValue();

    if (value == null) {
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return !reader.isCurrentValueNull();
        }
      };
    }

    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return reader.getValue().compareTo(value) != 0;
      }
    };
  }

  @Override
  public <T extends Comparable<T>> RecordPredicate visit(Lt<T> lt) {
    final GenericColumnReader<T> reader = getColumnReader(lt.getColumn());
    final T value = lt.getValue();

    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return !reader.isCurrentValueNull() && reader.getValue().compareTo(value) < 0;
      }
    };
  }

  @Override
  public <T extends Comparable<T>> RecordPredicate visit(LtEq<T> ltEq) {
    final GenericColumnReader<T> reader = getColumnReader(ltEq.getColumn());
    final T value = ltEq.getValue();

    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return !reader.isCurrentValueNull() && reader.getValue().compareTo(value) <= 0;
      }
    };
  }

  @Override
  public <T extends Comparable<T>> RecordPredicate visit(Gt<T> gt) {
    final GenericColumnReader<T> reader = getColumnReader(gt.getColumn());
    final T value = gt.getValue();

    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return !reader.isCurrentValueNull() && reader.getValue().compareTo(value) > 0;
      }
    };
  }

  @Override
  public <T extends Comparable<T>> RecordPredicate visit(GtEq<T> gtEq) {
    final GenericColumnReader<T> reader = getColumnReader(gtEq.getColumn());
    final T value = gtEq.getValue();

    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return !reader.isCurrentValueNull() && reader.getValue().compareTo(value) >= 0;
      }
    };
  }

  @Override
  public RecordPredicate visit(And and) {
    final RecordPredicate left = and.getLeft().accept(this);
    final RecordPredicate right = and.getRight().accept(this);
    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return left.isMatch() && right.isMatch();
      }
    };
  }

  @Override
  public RecordPredicate visit(Or or) {
    final RecordPredicate left = or.getLeft().accept(this);
    final RecordPredicate right = or.getRight().accept(this);
    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return left.isMatch() || right.isMatch();
      }
    };
  }

  @Override
  public RecordPredicate visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through CollapseLogicalNots? " + not);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RecordPredicate visit(UserDefined<T, U> udp) {
    final GenericColumnReader<T> reader = getColumnReader(udp.getColumn());
    final U predicate = udp.getUserDefinedPredicate();

    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return predicate.keep(reader.getValueOrNull());
      }
    };
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RecordPredicate visit(final LogicalNotUserDefined<T, U> udp) {
    final GenericColumnReader<T> reader = getColumnReader(udp.getUserDefined().getColumn());
    final U predicate = udp.getUserDefined().getUserDefinedPredicate();
    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return !predicate.keep(reader.getValueOrNull());
      }
    };
  }

}