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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetPojoTest {
  @Test
  public void testSimpleObjectReadWrite() throws Exception {
    List list = new ArrayList();

    boolean bool = true;
    byte b = (byte) 1;
    char c = 2;
    short s = 3;
    int i = 4;
    long l = 5;
    float f = 6.0f;
    double d = 7.0d;

    list.add(
      new TestClass(
        bool, bool, b, b, c, c, s, s, i, i, l, l, f, f, d, d, TestEnum.One, "Hello", "Hello".getBytes()
      )
    );

    List results = RoundTripUsingMR.writeAndRead(list, TestClass.class);

    Assert.assertEquals(list, results);
  }

  @Test
  public void testSimpleMapClass() throws Exception {
    List list = new ArrayList();

    Map<Integer, Integer> ints = new HashMap<Integer, Integer>();

    ints.put(1, 1);
    ints.put(2, 2);

    TestMapClasses.TestIntegerKeysValuesMap mapClass = new TestMapClasses.TestIntegerKeysValuesMap();
    mapClass.setMap(ints);

    list.add(mapClass);

    List results = RoundTripUsingMR.writeAndRead(list, TestMapClasses.TestIntegerKeysValuesMap.class);

    Assert.assertEquals(list, results);
  }

  @Test
  public void testListsWithIntegers() throws Exception {
    List<Integer> list = new ArrayList<Integer>();

    for (int i = 0; i < 5; i++) {
      if(i % 2 == 0) {
        list.add(null);
      } else {
        list.add(i);
      }
    }

    TestListClasses.TestListOfInts listClass = new TestListClasses.TestListOfInts();

    listClass.setInts(list);

    List values = new ArrayList();
    values.add(listClass);
    List results = RoundTripUsingMR.writeAndRead(values, TestListClasses.TestListOfInts.class);

    Assert.assertEquals(values, results);
  }

  @Test
  public void testMapsWithComplexKeysAndValues() throws Exception {
    Map<TestClass, TestClass> map = new HashMap<TestClass, TestClass>();

    boolean bool = true;
    byte b = (byte) 1;
    char c = 2;
    short s = 3;
    int i = 4;
    long l = 5;
    float f = 6.0f;
    double d = 7.0d;


    boolean bool2 = false;
    byte b2 = (byte) 2;
    char c2 = 3;
    short s2 = 4;
    int i2 = 5;
    long l2 = 6;
    float f2 = 7.0f;
    double d2 = 8.0d;

    TestClass first = new TestClass(
      bool, bool, b, b, c, c, s, s, i, i, l, l, f, f, d, d, TestEnum.One, "Hello", "Hello".getBytes()
    );

    TestClass second = new TestClass(
      bool2, bool2, b2, b2, c2, c2, s2, s2, i2, i2, l2, l2, f2, f2, d2, d2, TestEnum.Two, "Hello2",
      "Hello2".getBytes()
    );

    List values = new ArrayList();

    map.put(
      first, first
    );

    map.put(
      second, second
    );

    TestMapClasses.TestComplexKeysValuesMap complexKeysValuesMapClass = new TestMapClasses.TestComplexKeysValuesMap();
    complexKeysValuesMapClass.setMap(map);

    values.add(complexKeysValuesMapClass);

    List results = RoundTripUsingMR.writeAndRead(values, TestMapClasses.TestComplexKeysValuesMap.class);

    Assert.assertEquals(values, results);
  }

  @Test
  public void testListsWithComplexObjects() throws Exception {
    List<TestClass> list = new ArrayList<TestClass>();

    boolean bool = true;
    byte b = (byte) 1;
    char c = 2;
    short s = 3;
    int i = 4;
    long l = 5;
    float f = 6.0f;
    double d = 7.0d;

    list.add(
      new TestClass(
        bool, bool, b, null, c, c, s, s, i, i, l, l, f, f, d, d, TestEnum.One, "Hello", "Hello".getBytes()
      )
    );
    list.add(new TestClass(
        bool, bool, b, b, c, c, s, s, i, i, l, l, f, f, d, d, TestEnum.One, "Hello", "Hello".getBytes()
      ));

    List values = new ArrayList();
    TestListClasses.TestListOfTestClasses listClass = new TestListClasses.TestListOfTestClasses();
    listClass.setTestClasses(list);

    values.add(listClass);

    List results = RoundTripUsingMR.writeAndRead(values, TestListClasses.TestListOfTestClasses.class);

    Assert.assertEquals(values, results);
  }

  @Test
  public void testIntsReadWrite() throws Exception {
    List list = new ArrayList();

    list.add(1);

    List results = RoundTripUsingMR.writeAndRead(
      list, int.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testLongsReadWrite() throws Exception {
    List list = new ArrayList();

    list.add(1L);

    List results = RoundTripUsingMR.writeAndRead(
      list, long.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testFloatsReadWrite() throws Exception {
    List list = new ArrayList();

    list.add(1.0f);

    List results = RoundTripUsingMR.writeAndRead(
      list, float.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testDoublesReadWrite() throws Exception {
    List list = new ArrayList();

    list.add(1.0d);

    List results = RoundTripUsingMR.writeAndRead(
      list, double.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testEnumsReadWrite() throws Exception {
    List list = new ArrayList();

    list.add(TestEnum.One);
    list.add(TestEnum.Two);

    List results = RoundTripUsingMR.writeAndRead(
      list, TestEnum.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testBytesReadWrite() throws Exception {
    List list = new ArrayList();

    list.add((byte) 1);

    List results = RoundTripUsingMR.writeAndRead(
      list, byte.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testShortsReadWrite() throws Exception {
    List list = new ArrayList();

    list.add((short) 1);

    List results = RoundTripUsingMR.writeAndRead(
      list, short.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testCharsReadWrite() throws Exception {
    List list = new ArrayList();

    list.add((char) 1);

    List results = RoundTripUsingMR.writeAndRead(
      list, char.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testBoolReadWrite() throws Exception {
    List list = new ArrayList();

    list.add(false);

    List results = RoundTripUsingMR.writeAndRead(
      list, boolean.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testStringsReadWrite() throws Exception {
    List list = new ArrayList();

    list.add("Hello Parquet!");

    List results = RoundTripUsingMR.writeAndRead(
      list, String.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testByteArraysReadWrite() throws Exception {
    List list = new ArrayList();

    list.add("Hello Parquet!".getBytes());

    List results = RoundTripUsingMR.writeAndRead(
      list, byte[].class
    );

    Assert.assertEquals(list.size(), results.size());

    for (int i = 0; i < results.size(); i++) {
      byte[] bytes = (byte[]) results.get(i);
      byte[] otherBytes = (byte[]) list.get(i);

      Assert.assertArrayEquals(bytes, otherBytes);
    }
  }

  @Test
  public void testBoolArraysReadWrite() throws Exception {
    List list = new ArrayList();

    boolean[] booleans = new boolean[5];

    for (int i = 0; i < 5; i++) {
      booleans[i] = i % 2 == 0;
    }

    list.add(booleans);

    List results = RoundTripUsingMR.writeAndRead(
      list, boolean[].class
    );

    for (int i = 0; i < results.size(); i++) {
      boolean[] bools = (boolean[]) results.get(i);
      boolean[] otherBools = (boolean[]) list.get(i);

      Assert.assertTrue(Arrays.equals(bools, otherBools));
    }
  }

  @Test
  public void testCharArraysReadWrite() throws Exception {
    List list = new ArrayList();

    char[] chars = new char[5];

    for (int i = 0; i < 5; i++) {
      chars[i] = (char) i;
    }

    list.add(chars);

    List results = RoundTripUsingMR.writeAndRead(
      list, char[].class
    );

    for (int i = 0; i < results.size(); i++) {
      char[] charsList = (char[]) results.get(i);
      char[] otherChars = (char[]) list.get(i);

      Assert.assertArrayEquals(charsList, otherChars);
    }
  }

  @Test
  public void testShortArraysReadWrite() throws Exception {
    List list = new ArrayList();

    short[] shorts = new short[5];

    for (int i = 0; i < 5; i++) {
      shorts[i] = (short) i;
    }

    list.add(shorts);

    List results = RoundTripUsingMR.writeAndRead(
      list, short[].class
    );

    for (int i = 0; i < results.size(); i++) {
      short[] shortsList = (short[]) results.get(i);
      short[] othershorts = (short[]) list.get(i);

      Assert.assertArrayEquals(shortsList, othershorts);
    }
  }

  @Test
  public void testIntArraysReadWrite() throws Exception {
    List list = new ArrayList();

    int[] ints = new int[5];

    for (int i = 0; i < 5; i++) {
      ints[i] = i;
    }

    list.add(ints);

    List results = RoundTripUsingMR.writeAndRead(
      list, int[].class
    );

    for (int i = 0; i < results.size(); i++) {
      int[] intsList = (int[]) results.get(i);
      int[] otherInts = (int[]) list.get(i);

      Assert.assertArrayEquals(intsList, otherInts);
    }
  }

  @Test
  public void testLongArraysReadWrite() throws Exception {
    List list = new ArrayList();

    long[] longs = new long[5];

    for (int i = 0; i < 5; i++) {
      longs[i] = i;
    }

    list.add(longs);

    List results = RoundTripUsingMR.writeAndRead(
      list, long[].class
    );

    for (int i = 0; i < results.size(); i++) {
      long[] longsList = (long[]) results.get(i);
      long[] otherLongs = (long[]) list.get(i);

      Assert.assertArrayEquals(longsList, otherLongs);
    }
  }

  @Test
  public void testFloatArraysReadWrite() throws Exception {
    List list = new ArrayList();

    float[] floats = new float[5];

    for (int i = 0; i < 5; i++) {
      floats[i] = i;
    }

    list.add(floats);

    List results = RoundTripUsingMR.writeAndRead(
      list, float[].class
    );

    for (int i = 0; i < results.size(); i++) {
      float[] floatsList = (float[]) results.get(i);
      float[] otherFloats = (float[]) list.get(i);

      Assert.assertArrayEquals(floatsList, otherFloats, 0.0f);
    }
  }

  @Test
  public void testDoubleArraysReadWrite() throws Exception {
    List list = new ArrayList();

    double[] doubles = new double[5];

    for (int i = 0; i < 5; i++) {
      doubles[i] = i;
    }

    list.add(doubles);

    List results = RoundTripUsingMR.writeAndRead(
      list, double[].class
    );

    for (int i = 0; i < results.size(); i++) {
      double[] doublesList = (double[]) results.get(i);
      double[] otherDoubles = (double[]) list.get(i);

      Assert.assertArrayEquals(doublesList, otherDoubles, 0.0d);
    }
  }

  @Test
  public void testStringArraysReadWrite() throws Exception {
    List list = new ArrayList();

    String[] strings = new String[5];

    for (int i = 0; i < 5; i++) {
      strings[i] = Integer.valueOf(i).toString();
    }

    strings[4] = null;
    list.add(strings);

    List results = RoundTripUsingMR.writeAndRead(
      list, String[].class
    );

    for (int i = 0; i < results.size(); i++) {
      String[] stringsList = (String[]) results.get(i);
      String[] otherStrings = (String[]) list.get(i);

      Assert.assertArrayEquals(stringsList, otherStrings);
    }
  }

  @Test
  public void testArrayOfComplexTypes() throws Exception {
    List list = new ArrayList();

    boolean bool = true;
    byte b = (byte) 1;
    char c = 2;
    short s = 3;
    int i = 4;
    long l = 5;
    float f = 6.0f;
    double d = 7.0d;

    TestClass[] testClasses = new TestClass[1];
    testClasses[0] =
      new TestClass(
        bool, bool, b, null, c, c, s, s, i, i, l, l, f, f, d, d, TestEnum.One, "Hello", "Hello".getBytes()
      );

    list.add(testClasses);

    List results = RoundTripUsingMR.writeAndRead(
      list, TestClass[].class
    );

    TestClass[] testClassList = (TestClass[]) results.get(0);
    TestClass[] otherTestClasses = (TestClass[]) list.get(0);

    Assert.assertArrayEquals(testClassList, otherTestClasses);
  }

  @Test
  public void testArrayOfEnumsReadWrite() throws Exception {
    List list = new ArrayList();

    TestEnum[] enums = new TestEnum[2];
    enums[0] = TestEnum.One;
    enums[1] = TestEnum.Two;

    list.add(enums);

    List results = RoundTripUsingMR.writeAndRead(
      list, TestEnum[].class
    );

    TestEnum[] testEnums = (TestEnum[]) results.get(0);
    TestEnum[] otherTestEnum = (TestEnum[]) list.get(0);

    Assert.assertArrayEquals(testEnums, otherTestEnum);
  }

  @Test
  public void testArrayOfRefBoolsReadWrite() throws Exception {
    List list = new ArrayList();

    Boolean[] booleans = new Boolean[5];

    for (int i = 0; i < 5; i++) {
      booleans[i] = i % 2 == 0;
    }

    booleans[4] = null;
    list.add(booleans);

    List results = RoundTripUsingMR.writeAndRead(
      list, Boolean[].class
    );

    for (int i = 0; i < results.size(); i++) {
      Boolean[] bools = (Boolean[]) results.get(i);
      Boolean[] otherBools = (Boolean[]) list.get(i);

      Assert.assertArrayEquals(bools, otherBools);
    }
  }

  @Test
  public void testNestedClasses() throws Exception {
    boolean bool = true;
    byte b = (byte) 1;
    char c = 2;
    short s = 3;
    int i = 4;
    long l = 5;
    float f = 6.0f;
    double d = 7.0d;


    boolean bool2 = false;
    byte b2 = (byte) 2;
    char c2 = 3;
    short s2 = 4;
    int i2 = 5;
    long l2 = 6;
    float f2 = 7.0f;
    double d2 = 8.0d;

    TestClass first = new TestClass(
      bool, bool, b, b, c, c, s, s, i, i, l, l, f, f, d, d, TestEnum.One, "Hello", "Hello".getBytes()
    );

    TestClass second = new TestClass(
      bool2, bool2, b2, b2, c2, c2, s2, s2, i2, i2, l2, l2, f2, f2, d2, d2, TestEnum.Two, "Hello2",
      "Hello2".getBytes()
    );

    List values = new ArrayList();

    Map<TestClass, TestClass> map = new HashMap<TestClass, TestClass>();
    map.put(
      first, second
    );
    List<TestClass> list = new ArrayList<TestClass>();
    list.add(
      first
    );

    TestListClasses.TestListOfTestClasses testListClasses = new TestListClasses.TestListOfTestClasses();
    testListClasses.setTestClasses(list);

    TestMapClasses.TestComplexKeysValuesMap testComplexKeysValuesMap = new TestMapClasses.TestComplexKeysValuesMap();
    testComplexKeysValuesMap.setMap(map);

    TestNestedClass testNestedClass = new TestNestedClass(
      first, testListClasses, testComplexKeysValuesMap
    );
    values.add(testNestedClass);

    List results = RoundTripUsingMR.writeAndRead(
      values, TestNestedClass.class
    );

    TestNestedClass testNestedClasses = (TestNestedClass) results.get(0);
    TestNestedClass otherTestNestedClasses = (TestNestedClass) values.get(0);

    Assert.assertEquals(testNestedClasses, otherTestNestedClasses);
  }

  @Test
  public void testSimpleMapClassTopLevel() throws Exception {
    List list = new ArrayList();

    Map<Integer, Integer> ints = new HashMap<Integer, Integer>();

    ints.put(1, 1);
    ints.put(2, 2);
    ints.put(null, null);

    list.add(ints);

    List results = RoundTripUsingMR.writeAndRead(
      list, HashMap.class, Integer.class, Integer.class
    );

    Assert.assertEquals(list, results);
  }

  @Test
  public void testSimpleListTopLevel() throws Exception {
    List<Integer> list = new ArrayList<Integer>();

    for (int i = 0; i < 5; i++) {
      list.add(i);
    }

    List values = new ArrayList();
    values.add(list);

    List results = RoundTripUsingMR.writeAndRead(
      values, ArrayList.class, Integer.class
    );

    Assert.assertEquals(values, results);
  }
}
