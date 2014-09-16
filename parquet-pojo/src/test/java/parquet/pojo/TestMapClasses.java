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
package parquet.pojo;

import java.util.HashMap;
import java.util.Map;

public class TestMapClasses {
  public static class TestIntegerKeysValuesMap {
    @ConcreteType(createAs = HashMap.class)
    private Map<Integer, Integer> map = new HashMap<Integer, Integer>();

    public Map<Integer, Integer> getMap() {
      return map;
    }

    public void setMap(Map<Integer, Integer> map) {
      this.map = map;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestIntegerKeysValuesMap that = (TestIntegerKeysValuesMap) o;

      if (map != null ? !map.equals(that.map) : that.map != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return map != null ? map.hashCode() : 0;
    }
  }

  public static class TestComplexKeysValuesMap {
    @ConcreteType(createAs = HashMap.class)
    private Map<TestClass, TestClass> map = new HashMap<TestClass, TestClass>();

    public Map<TestClass, TestClass> getMap() {
      return map;
    }

    public void setMap(Map<TestClass, TestClass> map) {
      this.map = map;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestComplexKeysValuesMap that = (TestComplexKeysValuesMap) o;

      if (map != null ? !map.equals(that.map) : that.map != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return map != null ? map.hashCode() : 0;
    }
  }
}
