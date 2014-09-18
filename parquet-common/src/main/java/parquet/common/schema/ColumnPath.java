/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.common.schema;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import parquet.common.internal.Canonicalizer;

import static parquet.Preconditions.checkNotNull;

public final class ColumnPath implements Iterable<String>, Serializable {

  private static Canonicalizer<ColumnPath> paths = new Canonicalizer<ColumnPath>() {
    @Override
    protected ColumnPath toCanonical(ColumnPath value) {
      String[] path = new String[value.p.length];
      for (int i = 0; i < value.p.length; i++) {
        path[i] = value.p[i].intern();
      }
      return new ColumnPath(path);
    }
  };

  public static ColumnPath fromDotString(String path) {
    checkNotNull(path, "path");
    return get(path.split("\\."));
  }

  public static ColumnPath get(String... path){
    return paths.canonicalize(new ColumnPath(path));
  }

  private final String[] p;

  private ColumnPath(String[] path) {
    this.p = path;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ColumnPath) {
      return Arrays.equals(p, ((ColumnPath)obj).p);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(p);
  }

  public String toDotString() {
    Iterator<String> iter = Arrays.asList(p).iterator();
    StringBuilder sb = new StringBuilder();
    while (iter.hasNext()) {
      sb.append(iter.next());
      if (iter.hasNext()) {
        sb.append('.');
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return Arrays.toString(p);
  }

  @Override
  public Iterator<String> iterator() {
    return Arrays.asList(p).iterator();
  }

  public int size() {
    return p.length;
  }

  public String[] toArray() {
    return p;
  }
}
