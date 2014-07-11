package parquet.filter2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import parquet.filter2.StreamingFilterPredicate.Atom;
import parquet.io.api.Binary;

/**
 * This class is auto-generated by parquet.filter2.StreamingFilterPredicateBuilderGenerator
 * Do not manually edit!
 *
 * Constructs a {@link parquet.filter2.StreamingFilterPredicate} from a {@link parquet.filter2.FilterPredicate}
 * This is how records are filtered during record assembly. This file is generated in order to avoid autoboxing.
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

 * TODO(alexlevenson): user defined functions still autobox however
 */
public class StreamingFilterPredicateBuilder implements Visitor<StreamingFilterPredicate> {

  private final Map<String, List<Atom>> atomsByColumn = new HashMap<String, List<Atom>>();

  private StreamingFilterPredicateBuilder() { }

  public StreamingFilterPredicate build(FilterPredicate pred) {
    return pred.accept(new StreamingFilterPredicateBuilder());
  }

  private void addAtom(String columnPath, Atom atom) {
    List<Atom> atoms = atomsByColumn.get(columnPath);
    if (atoms == null) {
      atoms = new ArrayList<Atom>();
      atomsByColumn.put(columnPath, atoms);
    }
    atoms.add(atom);
  }

  public Map<String, List<Atom>> getAtomsByColumn() {
    return atomsByColumn;
  }

  @Override
  public <T extends Comparable<T>> StreamingFilterPredicate visit(Eq<T> pred) {
    String columnPath = pred.getColumn().getColumnPath();
    Class<T> clazz = pred.getColumn().getColumnType();

    Atom atom = null;

    if (clazz.equals(Integer.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(int value) {
            setResult(false);
          }
        };
      } else {
        final int target = (Integer) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(int value) {
            setResult(value == target);
          }
        };
      }
    }

    if (clazz.equals(Long.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(long value) {
            setResult(false);
          }
        };
      } else {
        final long target = (Long) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(long value) {
            setResult(value == target);
          }
        };
      }
    }

    if (clazz.equals(Boolean.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(boolean value) {
            setResult(false);
          }
        };
      } else {
        final boolean target = (Boolean) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(boolean value) {
            setResult(value == target);
          }
        };
      }
    }

    if (clazz.equals(Float.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(float value) {
            setResult(false);
          }
        };
      } else {
        final float target = (Float) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(float value) {
            setResult(value == target);
          }
        };
      }
    }

    if (clazz.equals(Double.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(double value) {
            setResult(false);
          }
        };
      } else {
        final double target = (Double) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(double value) {
            setResult(value == target);
          }
        };
      }
    }

    if (clazz.equals(Binary.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(Binary value) {
            setResult(false);
          }
        };
      } else {
        final Binary target = (Binary) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(Binary value) {
            setResult(value.compareTo(target) == 0 );
          }
        };
      }
    }

    if (atom == null) {
      throw new IllegalArgumentException("Encountered unknown type " + clazz);
    }

    addAtom(columnPath, atom);
    return atom;
  }

  @Override
  public <T extends Comparable<T>> StreamingFilterPredicate visit(NotEq<T> pred) {
    String columnPath = pred.getColumn().getColumnPath();
    Class<T> clazz = pred.getColumn().getColumnType();

    Atom atom = null;

    if (clazz.equals(Integer.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(int value) {
            setResult(true);
          }
        };
      } else {
        final int target = (Integer) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(int value) {
            setResult(value != target);
          }
        };
      }
    }

    if (clazz.equals(Long.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(long value) {
            setResult(true);
          }
        };
      } else {
        final long target = (Long) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(long value) {
            setResult(value != target);
          }
        };
      }
    }

    if (clazz.equals(Boolean.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(boolean value) {
            setResult(true);
          }
        };
      } else {
        final boolean target = (Boolean) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(boolean value) {
            setResult(value != target);
          }
        };
      }
    }

    if (clazz.equals(Float.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(float value) {
            setResult(true);
          }
        };
      } else {
        final float target = (Float) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(float value) {
            setResult(value != target);
          }
        };
      }
    }

    if (clazz.equals(Double.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(double value) {
            setResult(true);
          }
        };
      } else {
        final double target = (Double) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(double value) {
            setResult(value != target);
          }
        };
      }
    }

    if (clazz.equals(Binary.class)) {
      if (pred.getValue() == null) {
        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(false);
          }

          @Override
          public void update(Binary value) {
            setResult(true);
          }
        };
      } else {
        final Binary target = (Binary) (Object) pred.getValue();

        atom = new Atom() {
          @Override
          public void updateNull() {
            setResult(true);
          }

          @Override
          public void update(Binary value) {
            setResult(value.compareTo(target) != 0);
          }
        };
      }
    }

    if (atom == null) {
      throw new IllegalArgumentException("Encountered unknown type " + clazz);
    }

    addAtom(columnPath, atom);
    return atom;
  }

  @Override
  public <T extends Comparable<T>> StreamingFilterPredicate visit(Lt<T> pred) {
    String columnPath = pred.getColumn().getColumnPath();
    Class<T> clazz = pred.getColumn().getColumnType();

    Atom atom = null;

    if (clazz.equals(Integer.class)) {
      final int target = (Integer) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(int value) {
          setResult(value < target);
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final long target = (Long) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(long value) {
          setResult(value < target);
        }
      };
    }

    if (clazz.equals(Boolean.class)) {
      throw new IllegalArgumentException("Operator < not supported for Boolean");
    }

    if (clazz.equals(Float.class)) {
      final float target = (Float) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(float value) {
          setResult(value < target);
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final double target = (Double) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(double value) {
          setResult(value < target);
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary target = (Binary) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(Binary value) {
          setResult(value.compareTo(target) < 0);
        }
      };
    }

    if (atom == null) {
      throw new IllegalArgumentException("Encountered unknown type " + clazz);
    }

    addAtom(columnPath, atom);
    return atom;
  }

  @Override
  public <T extends Comparable<T>> StreamingFilterPredicate visit(LtEq<T> pred) {
    String columnPath = pred.getColumn().getColumnPath();
    Class<T> clazz = pred.getColumn().getColumnType();

    Atom atom = null;

    if (clazz.equals(Integer.class)) {
      final int target = (Integer) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(int value) {
          setResult(value <= target);
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final long target = (Long) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(long value) {
          setResult(value <= target);
        }
      };
    }

    if (clazz.equals(Boolean.class)) {
      throw new IllegalArgumentException("Operator <= not supported for Boolean");
    }

    if (clazz.equals(Float.class)) {
      final float target = (Float) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(float value) {
          setResult(value <= target);
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final double target = (Double) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(double value) {
          setResult(value <= target);
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary target = (Binary) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(Binary value) {
          setResult(value.compareTo(target) <= 0);
        }
      };
    }

    if (atom == null) {
      throw new IllegalArgumentException("Encountered unknown type " + clazz);
    }

    addAtom(columnPath, atom);
    return atom;
  }

  @Override
  public <T extends Comparable<T>> StreamingFilterPredicate visit(Gt<T> pred) {
    String columnPath = pred.getColumn().getColumnPath();
    Class<T> clazz = pred.getColumn().getColumnType();

    Atom atom = null;

    if (clazz.equals(Integer.class)) {
      final int target = (Integer) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(int value) {
          setResult(value > target);
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final long target = (Long) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(long value) {
          setResult(value > target);
        }
      };
    }

    if (clazz.equals(Boolean.class)) {
      throw new IllegalArgumentException("Operator > not supported for Boolean");
    }

    if (clazz.equals(Float.class)) {
      final float target = (Float) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(float value) {
          setResult(value > target);
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final double target = (Double) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(double value) {
          setResult(value > target);
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary target = (Binary) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(Binary value) {
          setResult(value.compareTo(target) > 0);
        }
      };
    }

    if (atom == null) {
      throw new IllegalArgumentException("Encountered unknown type " + clazz);
    }

    addAtom(columnPath, atom);
    return atom;
  }

  @Override
  public <T extends Comparable<T>> StreamingFilterPredicate visit(GtEq<T> pred) {
    String columnPath = pred.getColumn().getColumnPath();
    Class<T> clazz = pred.getColumn().getColumnType();

    Atom atom = null;

    if (clazz.equals(Integer.class)) {
      final int target = (Integer) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(int value) {
          setResult(value >= target);
        }
      };
    }

    if (clazz.equals(Long.class)) {
      final long target = (Long) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(long value) {
          setResult(value >= target);
        }
      };
    }

    if (clazz.equals(Boolean.class)) {
      throw new IllegalArgumentException("Operator >= not supported for Boolean");
    }

    if (clazz.equals(Float.class)) {
      final float target = (Float) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(float value) {
          setResult(value >= target);
        }
      };
    }

    if (clazz.equals(Double.class)) {
      final double target = (Double) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(double value) {
          setResult(value >= target);
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      final Binary target = (Binary) (Object) pred.getValue();

      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(false);
        }

        @Override
        public void update(Binary value) {
          setResult(value.compareTo(target) >= 0);
        }
      };
    }

    if (atom == null) {
      throw new IllegalArgumentException("Encountered unknown type " + clazz);
    }

    addAtom(columnPath, atom);
    return atom;
  }

  @Override
  public StreamingFilterPredicate visit(And and) {
    return new parquet.filter2.StreamingFilterPredicate.And(and.getLeft().accept(this), and.getRight().accept(this));
  }

  @Override
  public StreamingFilterPredicate visit(Or or) {
    return new parquet.filter2.StreamingFilterPredicate.Or(or.getLeft().accept(this), or.getRight().accept(this));
  }

  @Override
  public StreamingFilterPredicate visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through CollapseLogicalNots? " + not);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> StreamingFilterPredicate visit(UserDefined<T, U> pred) {
    String columnPath = pred.getColumn().getColumnPath();
    Class<T> clazz = pred.getColumn().getColumnType();

    Atom atom = null;

    final U udp = pred.getUserDefinedPredicate();

    if (clazz.equals(Integer.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(int value) {
          setResult(udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Long.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(long value) {
          setResult(udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Boolean.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(boolean value) {
          setResult(udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Float.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(float value) {
          setResult(udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Double.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(double value) {
          setResult(udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(Binary value) {
          setResult(udp.keep((T) (Object) value));
        }
      };
    }

    if (atom == null) {
      throw new IllegalArgumentException("Encountered unknown type " + clazz);
    }

    addAtom(columnPath, atom);
    return atom;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> StreamingFilterPredicate visit(LogicalNotUserDefined<T, U> notPred) {
    UserDefined<T, U> pred = notPred.getUserDefined();
    String columnPath = pred.getColumn().getColumnPath();
    Class<T> clazz = pred.getColumn().getColumnType();

    Atom atom = null;

    final U udp = pred.getUserDefinedPredicate();

    if (clazz.equals(Integer.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(!udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(int value) {
          setResult(!udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Long.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(!udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(long value) {
          setResult(!udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Boolean.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(!udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(boolean value) {
          setResult(!udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Float.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(!udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(float value) {
          setResult(!udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Double.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(!udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(double value) {
          setResult(!udp.keep((T) (Object) value));
        }
      };
    }

    if (clazz.equals(Binary.class)) {
      atom = new Atom() {
        @Override
        public void updateNull() {
          setResult(!udp.keep(null));
        }

        @SuppressWarnings("unchecked")
        @Override
        public void update(Binary value) {
          setResult(!udp.keep((T) (Object) value));
        }
      };
    }

    if (atom == null) {
      throw new IllegalArgumentException("Encountered unknown type " + clazz);
    }

    addAtom(columnPath, atom);
    return atom;
  }

}
