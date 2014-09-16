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

import java.util.ArrayList;
import java.util.List;

public class TestListClasses {
  public static class TestListOfTestClasses {
    @ConcreteType(createAs = ArrayList.class)
    private List<TestClass> testClasses = new ArrayList<TestClass>();

    public List<TestClass> getTestClasses() {
      return testClasses;
    }

    public void setTestClasses(List<TestClass> testClasses) {
      this.testClasses = testClasses;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestListOfTestClasses that = (TestListOfTestClasses) o;

      if (testClasses != null ? !testClasses.equals(that.testClasses) : that.testClasses != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return testClasses != null ? testClasses.hashCode() : 0;
    }
  }

  public static class TestListOfInts {
    @ConcreteType(createAs = ArrayList.class)
    private List<Integer> ints = new ArrayList<Integer>();

    public List<Integer> getInts() {
      return ints;
    }

    public void setInts(List<Integer> ints) {
      this.ints = ints;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestListOfInts that = (TestListOfInts) o;

      if (ints != null ? !ints.equals(that.ints) : that.ints != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return ints != null ? ints.hashCode() : 0;
    }
  }
}
