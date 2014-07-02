package parquet.filter2;

import java.util.HashMap;
import java.util.Map;

import parquet.bytes.BytesUtils;
import parquet.column.ColumnReader;
import parquet.filter2.FilterPredicate.Visitor;
import parquet.filter2.FilterPredicateOperators.And;
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
import parquet.filter2.UserDefinedPredicates.BinaryUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.DoubleUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.FloatUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.IntUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.LongUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.StringUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.UserDefinedPredicate;
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

  @Override
  public <T> RecordPredicate visit(Eq<T> eq) {

    Class<T> clazz = eq.getColumn().getColumnType();

    final ColumnReader reader = columns.get(eq.getColumn().getColumnPath());

    if (clazz.equals(Integer.class)) {
      final Integer value = (Integer) eq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getInteger() == value;
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final Long value = (Long) eq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getLong() == value;
        }
      };
    }

    if (clazz.equals(Float.class)) {
      final Float value = (Float) eq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getFloat() == value;
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final Double value = (Double) eq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getDouble() == value;
        }
      };
    }

    if (clazz.equals(Boolean.class)) {
      final Boolean value = (Boolean) eq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getBoolean() == value;
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary value = (Binary) eq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.equals(reader.getBinary());
        }
      };
    }

    if (clazz.equals(String.class)) {
      String strValue = (String) eq.getValue();
      final Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.equals(reader.getBinary());
        }
      };
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
  }

  @Override
  public <T> RecordPredicate visit(NotEq<T> notEq) {
    Class<T> clazz = notEq.getColumn().getColumnType();

    final ColumnReader reader = columns.get(notEq.getColumn().getColumnPath());

    if (clazz.equals(Integer.class)) {
      final Integer value = (Integer) notEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getInteger() != value;
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final Long value = (Long) notEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getLong() != value;
        }
      };
    }

    if (clazz.equals(Float.class)) {
      final Float value = (Float) notEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getFloat() != value;
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final Double value = (Double) notEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getDouble() != value;
        }
      };
    }

    if (clazz.equals(Boolean.class)) {
      final Boolean value = (Boolean) notEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getBoolean() != value;
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary value = (Binary) notEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return !value.equals(reader.getBinary());
        }
      };
    }

    if (clazz.equals(String.class)) {
      String strValue = (String) notEq.getValue();
      final Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return !value.equals(reader.getBinary());
        }
      };
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());

  }

  @Override
  public <T> RecordPredicate visit(Lt<T> lt) {
    Class<T> clazz = lt.getColumn().getColumnType();

    final ColumnReader reader = columns.get(lt.getColumn().getColumnPath());

    if (clazz.equals(Integer.class)) {
      final Integer value = (Integer) lt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getInteger() < value;
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final Long value = (Long) lt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getLong() < value;
        }
      };
    }

    if (clazz.equals(Float.class)) {
      final Float value = (Float) lt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getFloat() < value;
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final Double value = (Double) lt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getDouble() < value;
        }
      };
    }

    // TODO(alexlevenson): < on boolean is nonsense
    if (clazz.equals(Boolean.class)) {
      final Boolean value = (Boolean) lt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBoolean()) < 0;
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary value = (Binary) lt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBinary()) < 0;
        }
      };
    }

    if (clazz.equals(String.class)) {
      String strValue = (String) lt.getValue();
      final Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBinary()) < 0;
        }
      };
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
  }

  @Override
  public <T> RecordPredicate visit(LtEq<T> ltEq) {
    Class<T> clazz = ltEq.getColumn().getColumnType();

    final ColumnReader reader = columns.get(ltEq.getColumn().getColumnPath());

    if (clazz.equals(Integer.class)) {
      final Integer value = (Integer) ltEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getInteger() <= value;
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final Long value = (Long) ltEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getLong() <= value;
        }
      };
    }

    if (clazz.equals(Float.class)) {
      final Float value = (Float) ltEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getFloat() <= value;
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final Double value = (Double) ltEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getDouble() <= value;
        }
      };
    }

    // TODO(alexlevenson): <= on boolean is nonsense
    if (clazz.equals(Boolean.class)) {
      final Boolean value = (Boolean) ltEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBoolean()) <= 0;
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary value = (Binary) ltEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBinary()) <= 0;
        }
      };
    }

    if (clazz.equals(String.class)) {
      String strValue = (String) ltEq.getValue();
      final Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBinary()) <= 0;
        }
      };
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
  }

  @Override
  public <T> RecordPredicate visit(Gt<T> gt) {
    Class<T> clazz = gt.getColumn().getColumnType();

    final ColumnReader reader = columns.get(gt.getColumn().getColumnPath());

    if (clazz.equals(Integer.class)) {
      final Integer value = (Integer) gt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getInteger() > value;
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final Long value = (Long) gt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getLong() > value;
        }
      };
    }

    if (clazz.equals(Float.class)) {
      final Float value = (Float) gt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getFloat() > value;
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final Double value = (Double) gt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getDouble() > value;
        }
      };
    }

    // TODO(alexlevenson): > on boolean is nonsense
    if (clazz.equals(Boolean.class)) {
      final Boolean value = (Boolean) gt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBoolean()) > 0;
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary value = (Binary) gt.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBinary()) > 0;
        }
      };
    }

    if (clazz.equals(String.class)) {
      String strValue = (String) gt.getValue();
      final Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBinary()) > 0;
        }
      };
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
  }

  @Override
  public <T> RecordPredicate visit(GtEq<T> gtEq) {
    Class<T> clazz = gtEq.getColumn().getColumnType();

    final ColumnReader reader = columns.get(gtEq.getColumn().getColumnPath());

    if (clazz.equals(Integer.class)) {
      final Integer value = (Integer) gtEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getInteger() >= value;
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final Long value = (Long) gtEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getLong() >= value;
        }
      };
    }

    if (clazz.equals(Float.class)) {
      final Float value = (Float) gtEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getFloat() >= value;
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final Double value = (Double) gtEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return reader.getDouble() >= value;
        }
      };
    }

    // TODO(alexlevenson): >= on boolean is nonsense
    if (clazz.equals(Boolean.class)) {
      final Boolean value = (Boolean) gtEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBoolean()) >= 0;
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary value = (Binary) gtEq.getValue();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBinary()) >= 0;
        }
      };
    }

    if (clazz.equals(String.class)) {
      String strValue = (String) gtEq.getValue();
      final Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return value.compareTo(reader.getBinary()) >= 0;
        }
      };
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
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
  public <T, U extends UserDefinedPredicate<T>> RecordPredicate visit(UserDefined<T, U> udp) {
    Class<T> clazz = udp.getColumn().getColumnType();

    final ColumnReader reader = columns.get(udp.getColumn().getColumnPath());

    if (clazz.equals(Integer.class)) {
      final IntUserDefinedPredicate f = (IntUserDefinedPredicate) udp.getUserDefinedPredicate();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return f.keep(reader.getInteger());
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final LongUserDefinedPredicate f = (LongUserDefinedPredicate) udp.getUserDefinedPredicate();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return f.keep(reader.getLong());
        }
      };
    }

    if (clazz.equals(Float.class)) {
      final FloatUserDefinedPredicate f = (FloatUserDefinedPredicate) udp.getUserDefinedPredicate();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return f.keep(reader.getFloat());
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final DoubleUserDefinedPredicate f = (DoubleUserDefinedPredicate) udp.getUserDefinedPredicate();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return f.keep(reader.getDouble());
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final BinaryUserDefinedPredicate f = (BinaryUserDefinedPredicate) udp.getUserDefinedPredicate();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return f.keep(reader.getBinary());
        }
      };
    }

    if (clazz.equals(String.class)) {
      final StringUserDefinedPredicate f = (StringUserDefinedPredicate) udp.getUserDefinedPredicate();
      return new RecordPredicate() {
        @Override
        public boolean isMatch() {
          return f.keep(reader.getBinary().toStringUsingUTF8());
        }
      };
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
  }

  @Override
  public <T, U extends UserDefinedPredicate<T>> RecordPredicate visit(final LogicalNotUserDefined<T, U> udp) {
    final RecordPredicate pred = udp.getUserDefined().accept(this);

    return new RecordPredicate() {
      @Override
      public boolean isMatch() {
        return !pred.isMatch();
      }
    };
  }
}