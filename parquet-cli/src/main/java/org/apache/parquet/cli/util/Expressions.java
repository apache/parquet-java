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

package org.apache.parquet.cli.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Expressions {
  private static final Pattern NUMERIC_RE = Pattern.compile("^\\d+$");

  public static Object select(Schema schema, Object datum, String path) {
    return select(schema, datum, Lists.newArrayList(parse(path)));
  }

  @SuppressWarnings("unchecked")
  private static Object select(Schema schema, Object datum, List<PathExpr> tokens) {
    if (tokens.isEmpty()) {
      return datum;
    }

    Preconditions.checkArgument(tokens.size() == 1, "Cannot return multiple values");
    PathExpr token = tokens.get(0);

    switch (schema.getType()) {
    case RECORD:
      if (!(datum instanceof GenericRecord) && "json".equals(schema.getName())) {
        // skip the placeholder record schema
        return select(schema.getField("value").schema(), datum, tokens);
      }
      Preconditions.checkArgument(token.type == PathExpr.Type.FIELD, "Cannot dereference records");
      Preconditions.checkArgument(datum instanceof GenericRecord, "Not a record: %s", datum);
      GenericRecord record = (GenericRecord) datum;
      Schema.Field field = schema.getField(token.value);
      Preconditions.checkArgument(field != null, "No such field '%s' in schema: %s", token.value, schema);
      return select(field.schema(), record.get(token.value), token.children);

    case MAP:
      Preconditions.checkArgument(datum instanceof Map, "Not a map: %s", datum);
      Map<Object, Object> map = (Map<Object, Object>) datum;
      Object value = map.get(token.value);
      if (value == null) {
        // try with a Utf8
        value = map.get(new Utf8(token.value));
      }
      return select(schema.getValueType(), value, token.children);

    case ARRAY:
      Preconditions.checkArgument(token.type == PathExpr.Type.DEREF, "Cannot access fields of an array");
      Preconditions.checkArgument(datum instanceof Collection, "Not an array: %s", datum);
      Preconditions.checkArgument(NUMERIC_RE.matcher(token.value).matches(), "Not an array index: %s", token.value);
      List<Object> list = (List<Object>) datum;
      return select(schema.getElementType(), list.get(Integer.parseInt(token.value)), token.children);

    case UNION:
      int branch = GenericData.get().resolveUnion(schema, datum);
      return select(schema.getTypes().get(branch), datum, tokens);

    default:
      throw new IllegalArgumentException("Cannot access child of primitive value: " + datum);
    }
  }

  /**
   * a.2.b[3]["key"] * optional (union with null) should be ignored * unions
   * should match by position number or short name (e.g. 2, user) * fields should
   * match by name * arrays are dereferenced by position [n] =&gt; schema is the
   * element schema * maps are dereferenced by key =&gt; schema is the value
   * schema
   *
   * @param schema an Avro schema
   * @param fieldPaths selected field paths
   * @return a filtered schema
   */
  public static Schema filterSchema(Schema schema, String... fieldPaths) {
    return filterSchema(schema, Lists.newArrayList(fieldPaths));
  }

  public static Schema filterSchema(Schema schema, List<String> fieldPaths) {
    if (fieldPaths == null) {
      return schema;
    }
    List<PathExpr> paths = merge(Lists.newArrayList(fieldPaths));
    return filter(schema, paths);
  }

  private static PathExpr parse(String path) {
    PathExpr expr = null;
    PathExpr last = null;
    boolean inDeref = false;
    boolean afterDeref = false;
    int valueStart = 0;
    for (int i = 0; i < path.length(); i += 1) {
      switch (path.charAt(i)) {
      case '.':
        Preconditions.checkState(valueStart != i || afterDeref, "Empty reference: ''");
        if (!inDeref) {
          if (valueStart != i) {
            PathExpr current = PathExpr.field(path.substring(valueStart, i));
            if (last != null) {
              last.children.add(current);
            } else {
              expr = current;
            }
            last = current;
          }
          valueStart = i + 1;
          afterDeref = false;
        }
        break;
      case '[':
        Preconditions.checkState(!inDeref, "Cannot nest [ within []");
        Preconditions.checkState(valueStart != i || afterDeref, "Empty reference: ''");
        if (valueStart != i) {
          PathExpr current = PathExpr.field(path.substring(valueStart, i));
          if (last != null) {
            last.children.add(current);
          } else {
            expr = current;
          }
          last = current;
        }
        valueStart = i + 1;
        inDeref = true;
        afterDeref = false;
        break;
      case ']':
        Preconditions.checkState(inDeref, "Cannot use ] without a starting [");
        Preconditions.checkState(valueStart != i, "Empty reference: ''");
        PathExpr current = PathExpr.deref(path.substring(valueStart, i));
        if (last != null) {
          last.children.add(current);
        } else {
          expr = current;
        }
        last = current;
        valueStart = i + 1;
        inDeref = false;
        afterDeref = true;
        break;
      default:
        Preconditions.checkState(!afterDeref, "Fields after [] must start with .");
      }
    }
    Preconditions.checkState(!inDeref, "Fields after [ must end with ]");
    if (valueStart < path.length()) {
      PathExpr current = PathExpr.field(path.substring(valueStart, path.length()));
      if (last != null) {
        last.children.add(current);
      } else {
        expr = current;
      }
    }
    return expr;
  }

  private static List<PathExpr> merge(List<String> fields) {
    List<PathExpr> paths = Lists.newArrayList();
    for (String field : fields) {
      merge(paths, parse(field));
    }
    return paths;
  }

  private static List<PathExpr> merge(List<PathExpr> tokens, PathExpr toAdd) {
    boolean merged = false;
    for (PathExpr token : tokens) {
      if ((token.type == toAdd.type) && (token.type == PathExpr.Type.DEREF || token.value.equals(toAdd.value))) {
        for (PathExpr child : toAdd.children) {
          merge(token.children, child);
        }
        merged = true;
      }
    }
    if (!merged) {
      tokens.add(toAdd);
    }
    return tokens;
  }

  private static Schema filter(Schema schema, List<PathExpr> exprs) {
    if (exprs.isEmpty()) {
      return schema;
    }

    switch (schema.getType()) {
    case RECORD:
      List<Schema.Field> fields = Lists.newArrayList();
      for (PathExpr expr : exprs) {
        Schema.Field field = schema.getField(expr.value);
        Preconditions.checkArgument(field != null, "Cannot find field '%s' in schema: %s", expr.value, schema);
        fields.add(new Schema.Field(expr.value, filter(field.schema(), expr.children), field.doc(), field.defaultVal(),
            field.order()));
      }
      return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), fields);

    case UNION:
      // Ignore schemas that are a union with null because there is another token
      if (schema.getTypes().size() == 2) {
        if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
          return filter(schema.getTypes().get(1), exprs);
        } else if (schema.getTypes().get(1).getType() == Schema.Type.NULL) {
          return filter(schema.getTypes().get(0), exprs);
        }
      }

      List<Schema> schemas = Lists.newArrayList();
      for (PathExpr expr : exprs) {
        schemas.add(filter(schema, expr));
      }

      if (schemas.size() > 1) {
        return Schema.createUnion(schemas);
      } else {
        return schemas.get(0);
      }

    case MAP:
      Preconditions.checkArgument(exprs.size() == 1, "Cannot find multiple children of map schema: %s", schema);
      return filter(schema, exprs.get(0));

    case ARRAY:
      Preconditions.checkArgument(exprs.size() == 1, "Cannot find multiple children of array schema: %s", schema);
      return filter(schema, exprs.get(0));

    default:
      throw new IllegalArgumentException(String.format("Cannot find child of primitive schema: %s", schema));
    }
  }

  private static Schema filter(Schema schema, PathExpr expr) {
    if (expr == null) {
      return schema;
    }

    switch (schema.getType()) {
    case RECORD:
      Preconditions.checkArgument(expr.type == PathExpr.Type.FIELD, "Cannot index a record: [%s]", expr.value);
      Schema.Field field = schema.getField(expr.value);
      if (field != null) {
        return filter(field.schema(), expr.children);
      } else {
        throw new IllegalArgumentException(
            String.format("Cannot find field '%s' in schema: %s", expr.value, schema.toString(true)));
      }

    case MAP:
      return Schema.createMap(filter(schema.getValueType(), expr.children));

    case ARRAY:
      Preconditions.checkArgument(expr.type == PathExpr.Type.DEREF, "Cannot find field '%s' in an array", expr.value);
      Preconditions.checkArgument(NUMERIC_RE.matcher(expr.value).matches(),
          "Cannot index array by non-numeric value '%s'", expr.value);
      return Schema.createArray(filter(schema.getElementType(), expr.children));

    case UNION:
      // TODO: this should only return something if the type can match rather than
      // explicitly
      // accessing parts of a union. when selecting data, unions are ignored.
      Preconditions.checkArgument(expr.type == PathExpr.Type.DEREF, "Cannot find field '%s' in a union", expr.value);
      List<Schema> options = schema.getTypes();
      if (NUMERIC_RE.matcher(expr.value).matches()) {
        // look up the option by position
        int i = Integer.parseInt(expr.value);
        if (i < options.size()) {
          return filter(options.get(i), expr.children);
        }
      } else {
        // look up the option by name
        for (Schema option : options) {
          if (expr.value.equalsIgnoreCase(option.getName())) {
            return filter(option, expr.children);
          }
        }
      }
      throw new IllegalArgumentException(String.format("Invalid union index '%s' for schema: %s", expr.value, schema));

    default:
      throw new IllegalArgumentException(String.format("Cannot find '%s' in primitive schema: %s", expr.value, schema));
    }
  }

  private static class PathExpr {
    enum Type {
      DEREF, FIELD
    }

    static PathExpr deref(String value) {
      return new PathExpr(Type.DEREF, value);
    }

    static PathExpr deref(String value, PathExpr child) {
      return new PathExpr(Type.DEREF, value, Lists.newArrayList(child));
    }

    static PathExpr field(String value) {
      return new PathExpr(Type.FIELD, value);
    }

    static PathExpr field(String value, PathExpr child) {
      return new PathExpr(Type.FIELD, value, Lists.newArrayList(child));
    }

    private final Type type;
    private final String value;
    private final List<PathExpr> children;

    PathExpr(Type type, String value) {
      this.type = type;
      this.value = value;
      this.children = Lists.newArrayList();
    }

    PathExpr(Type type, String value, List<PathExpr> children) {
      this.type = type;
      this.value = value;
      this.children = children;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PathExpr pathExpr = (PathExpr) o;

      if (type != pathExpr.type)
        return false;
      if (value != null ? !value.equals(pathExpr.value) : pathExpr.value != null)
        return false;
      return children != null ? children.equals(pathExpr.children) : pathExpr.children == null;
    }

    @Override
    public int hashCode() {
      int result = type != null ? type.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (children != null ? children.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("type", type).add("value", value).add("children", children)
          .toString();
    }
  }
}
