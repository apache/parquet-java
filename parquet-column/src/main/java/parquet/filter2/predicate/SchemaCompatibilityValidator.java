/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.filter2.predicate;

import java.util.HashMap;
import java.util.Map;

import parquet.column.ColumnDescriptor;
import parquet.common.schema.ColumnPath;
import parquet.filter2.predicate.Operators.And;
import parquet.filter2.predicate.Operators.Column;
import parquet.filter2.predicate.Operators.ColumnFilterPredicate;
import parquet.filter2.predicate.Operators.Eq;
import parquet.filter2.predicate.Operators.Gt;
import parquet.filter2.predicate.Operators.GtEq;
import parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import parquet.filter2.predicate.Operators.Lt;
import parquet.filter2.predicate.Operators.LtEq;
import parquet.filter2.predicate.Operators.Not;
import parquet.filter2.predicate.Operators.NotEq;
import parquet.filter2.predicate.Operators.Or;
import parquet.filter2.predicate.Operators.UserDefined;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;

import static parquet.Preconditions.checkArgument;
import static parquet.Preconditions.checkNotNull;

/**
 * Inspects the column types found in the provided {@link FilterPredicate} and compares them
 * to the actual schema found in the parquet file. If the provided predicate's types are
 * not consistent with the file schema, and IllegalArgumentException is thrown.
 *
 * Ideally, all this would be checked at compile time, and this class wouldn't be needed.
 * If we can come up with a way to do that, we should.
 *
 * This class is stateful, cannot be reused, and is not thread safe.
 *
 * TODO: detect if a column is optional or required and validate that eq(null)
 * TODO: is not called on required fields (is that too strict?)
 * TODO: (https://issues.apache.org/jira/browse/PARQUET-44)
 */
public class SchemaCompatibilityValidator implements FilterPredicate.Visitor<Void> {

  public static void validate(FilterPredicate predicate, MessageType schema) {
    checkNotNull(predicate, "predicate");
    checkNotNull(schema, "schema");
    predicate.accept(new SchemaCompatibilityValidator(schema));
  }

  // A map of column name to the type the user supplied for this column.
  // Used to validate that the user did not provide different types for the same
  // column.
  private final Map<ColumnPath, Class<?>> columnTypesEncountered = new HashMap<ColumnPath, Class<?>>();

  // the columns (keyed by path) according to the file's schema. This is the source of truth, and
  // we are validating that what the user provided agrees with these.
  private final Map<ColumnPath, ColumnDescriptor> columnsAccordingToSchema = new HashMap<ColumnPath, ColumnDescriptor>();

  // the original type of a column, keyed by path
  private final Map<ColumnPath, OriginalType> originalTypes = new HashMap<ColumnPath, OriginalType>();

  private SchemaCompatibilityValidator(MessageType schema) {

    for (ColumnDescriptor cd : schema.getColumns()) {
      ColumnPath columnPath = ColumnPath.get(cd.getPath());
      columnsAccordingToSchema.put(columnPath, cd);

      OriginalType ot = schema.getType(cd.getPath()).getOriginalType();
      if (ot != null) {
        originalTypes.put(columnPath, ot);
      }
    }
  }

  @Override
  public <T extends Comparable<T>> Void visit(Eq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(NotEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(Lt<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(LtEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(Gt<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(GtEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public Void visit(And and) {
    and.getLeft().accept(this);
    and.getRight().accept(this);
    return null;
  }

  @Override
  public Void visit(Or or) {
    or.getLeft().accept(this);
    or.getRight().accept(this);
    return null;
  }

  @Override
  public Void visit(Not not) {
    not.getPredicate().accept(this);
    return null;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Void visit(UserDefined<T, U> udp) {
    validateColumn(udp.getColumn());
    return null;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Void visit(LogicalNotUserDefined<T, U> udp) {
    return udp.getUserDefined().accept(this);
  }

  private <T extends Comparable<T>> void validateColumnFilterPredicate(ColumnFilterPredicate<T> pred) {
    validateColumn(pred.getColumn());
  }

  private <T extends Comparable<T>> void validateColumn(Column<T> column) {
    ColumnPath path = column.getColumnPath();

    Class<?> alreadySeen = columnTypesEncountered.get(path);
    if (alreadySeen != null && !alreadySeen.equals(column.getColumnType())) {
      throw new IllegalArgumentException("Column: "
          + path.toDotString()
          + " was provided with different types in the same predicate."
          + " Found both: (" + alreadySeen + ", " + column.getColumnType() + ")");
    }

    if (alreadySeen == null) {
      columnTypesEncountered.put(path, column.getColumnType());
    }

    ColumnDescriptor descriptor = getColumnDescriptor(path);

    if (descriptor.getMaxRepetitionLevel() > 0) {
      throw new IllegalArgumentException("FilterPredicates do not currently support repeated columns. "
          + "Column " + path.toDotString() + " is repeated.");
    }

    ValidTypeMap.assertTypeValid(column, descriptor.getType(), originalTypes.get(path));
  }

  private ColumnDescriptor getColumnDescriptor(ColumnPath columnPath) {
    ColumnDescriptor cd = columnsAccordingToSchema.get(columnPath);
    checkArgument(cd != null, "Column " + columnPath + " was not found in schema!");
    return cd;
  }
}
