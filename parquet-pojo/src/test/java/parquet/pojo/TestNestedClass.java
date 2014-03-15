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

public class TestNestedClass {
  private TestClass testClass;
  private TestListClasses.TestListOfTestClasses testListClasses;
  private TestMapClasses.TestComplexKeysValuesMap testMapClasses;

  public TestNestedClass() {

  }

  public TestNestedClass(
    TestClass testClass, TestListClasses.TestListOfTestClasses testListClasses,
    TestMapClasses.TestComplexKeysValuesMap testMapClasses
  ) {
    this.testClass = testClass;
    this.testListClasses = testListClasses;
    this.testMapClasses = testMapClasses;
  }

  public TestClass getTestClass() {
    return testClass;
  }

  public void setTestClass(TestClass testClass) {
    this.testClass = testClass;
  }

  public TestListClasses.TestListOfTestClasses getTestListClasses() {
    return testListClasses;
  }

  public void setTestListClasses(TestListClasses.TestListOfTestClasses testListClasses) {
    this.testListClasses = testListClasses;
  }

  public TestMapClasses.TestComplexKeysValuesMap getTestMapClasses() {
    return testMapClasses;
  }

  public void setTestMapClasses(TestMapClasses.TestComplexKeysValuesMap testMapClasses) {
    this.testMapClasses = testMapClasses;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestNestedClass that = (TestNestedClass) o;

    if (testClass != null ? !testClass.equals(that.testClass) : that.testClass != null) {
      return false;
    }
    if (testListClasses != null ? !testListClasses.equals(that.testListClasses) : that.testListClasses != null) {
      return false;
    }
    if (testMapClasses != null ? !testMapClasses.equals(that.testMapClasses) : that.testMapClasses != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = testClass != null ? testClass.hashCode() : 0;
    result = 31 * result + (testListClasses != null ? testListClasses.hashCode() : 0);
    result = 31 * result + (testMapClasses != null ? testMapClasses.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TestNestedClass{" +
      "testClass=" + testClass +
      ", testListClasses=" + testListClasses +
      ", testMapClasses=" + testMapClasses +
      '}';
  }
}
