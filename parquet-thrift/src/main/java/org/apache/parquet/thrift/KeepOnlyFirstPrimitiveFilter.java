package org.apache.parquet.thrift;

import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.projection.FieldsPath;
import org.apache.parquet.thrift.projection.ThriftProjectionException;

/**
 * A {@link FieldProjectionFilter} that keeps only the first primitive field
 * that it encounters.
 */
class KeepOnlyFirstPrimitiveFilter implements FieldProjectionFilter {
  private boolean found = false;

  @Override
  public boolean keep(FieldsPath path) {
    if (found) {
      return false;
    }

    found = true;
    return true;
  }

  @Override
  public void assertNoUnmatchedPatterns() throws ThriftProjectionException { }
}
