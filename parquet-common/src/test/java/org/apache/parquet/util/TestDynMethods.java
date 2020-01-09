/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.util;

import org.apache.parquet.TestUtils;
import org.apache.parquet.util.Concatenator.SomeCheckedException;
import org.junit.Assert;
import org.junit.Test;
import java.util.concurrent.Callable;

public class TestDynMethods {
  @Test
  public void testNoImplCall() {
    final DynMethods.Builder builder = new DynMethods.Builder("concat");

    TestUtils.assertThrows("Checked build should throw NoSuchMethodException",
        NoSuchMethodException.class, (Callable) builder::buildChecked);

    TestUtils.assertThrows("Normal build should throw RuntimeException",
        RuntimeException.class, (Callable) builder::build);
  }

  @Test
  public void testMissingClass() {
    final DynMethods.Builder builder = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class);

    TestUtils.assertThrows("Checked build should throw NoSuchMethodException",
        NoSuchMethodException.class, (Callable) builder::buildChecked);

    TestUtils.assertThrows("Normal build should throw RuntimeException",
        RuntimeException.class, (Runnable) builder::build);
  }

  @Test
  public void testMissingMethod() {
    final DynMethods.Builder builder = new DynMethods.Builder("concat")
        .impl(Concatenator.class, "cat2strings", String.class, String.class);

    TestUtils.assertThrows("Checked build should throw NoSuchMethodException",
        NoSuchMethodException.class, (Callable) builder::buildChecked);

    TestUtils.assertThrows("Normal build should throw RuntimeException",
        RuntimeException.class, (Runnable) builder::build);
  }

  @Test
  public void testFirstImplReturned() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat2 = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .impl(Concatenator.class, String.class, String.class, String.class)
        .buildChecked();

    Assert.assertEquals("Should call the 2-arg version successfully",
        "a-b", cat2.invoke(obj, "a", "b"));

    Assert.assertEquals("Should ignore extra arguments",
        "a-b", cat2.invoke(obj, "a", "b", "c"));

    DynMethods.UnboundMethod cat3 = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .build();

    Assert.assertEquals("Should call the 3-arg version successfully",
        "a-b-c", cat3.invoke(obj, "a", "b", "c"));

    Assert.assertEquals("Should call the 3-arg version null padding",
        "a-b-null", cat3.invoke(obj, "a", "b"));
  }

  @Test
  public void testVarArgs() throws Exception {
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String[].class)
        .buildChecked();

    Assert.assertEquals("Should use the varargs version", "abcde",
        cat.invokeChecked(
            new Concatenator(),
            (Object) new String[] {"a", "b", "c", "d", "e"}));

    Assert.assertEquals("Should use the varargs version", "abcde",
        cat.bind(new Concatenator())
            .invokeChecked((Object) new String[] {"a", "b", "c", "d", "e"}));
  }

  @Test
  public void testIncorrectArguments() throws Exception {
    final Concatenator obj = new Concatenator("-");
    final DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked();

    TestUtils.assertThrows("Should fail if non-string arguments are passed",
        IllegalArgumentException.class, () -> cat.invoke(obj, 3, 4));

    TestUtils.assertThrows("Should fail if non-string arguments are passed",
        IllegalArgumentException.class, () -> cat.invokeChecked(obj, 3, 4));
  }

  @Test
  public void testExceptionThrown() throws Exception {
    final SomeCheckedException exc = new SomeCheckedException();
    final Concatenator obj = new Concatenator("-");
    final DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, Exception.class)
        .buildChecked();

    TestUtils.assertThrows("Should re-throw the exception",
        SomeCheckedException.class, () -> cat.invokeChecked(obj, exc));

    TestUtils.assertThrows("Should wrap the exception in RuntimeException",
        RuntimeException.class, () -> cat.invoke(obj, exc));
  }

  @Test
  public void testNameChange() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat = new DynMethods.Builder("cat")
        .impl(Concatenator.class, "concat", String.class, String.class)
        .buildChecked();

    Assert.assertEquals("Should find 2-arg concat method",
        "a-b", cat.invoke(obj, "a", "b"));
  }

  @Test
  public void testStringClassname() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class.getName(), String.class, String.class)
        .buildChecked();

    Assert.assertEquals("Should find 2-arg concat method",
        "a-b", cat.invoke(obj, "a", "b"));
  }

  @Test
  public void testHiddenMethod() throws Exception {
    Concatenator obj = new Concatenator("-");

    TestUtils.assertThrows("Should fail to find hidden method",
        NoSuchMethodException.class, () -> new DynMethods.Builder("setSeparator")
            .impl(Concatenator.class, String.class)
            .buildChecked());

    DynMethods.UnboundMethod changeSep = new DynMethods.Builder("setSeparator")
        .hiddenImpl(Concatenator.class, String.class)
        .buildChecked();

    Assert.assertNotNull("Should find hidden method with hiddenImpl",
        changeSep);

    changeSep.invokeChecked(obj, "/");

    Assert.assertEquals("Should use separator / instead of -",
        "a/b", obj.concat("a", "b"));
  }

  @Test
  public void testBoundMethod() throws Exception {
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked();

    // Unbound methods can be bound multiple times
    DynMethods.BoundMethod dashCat = cat.bind(new Concatenator("-"));
    DynMethods.BoundMethod underCat = cat.bind(new Concatenator("_"));

    Assert.assertEquals("Should use '-' object without passing",
        "a-b", dashCat.invoke("a", "b"));
    Assert.assertEquals("Should use '_' object without passing",
        "a_b", underCat.invoke("a", "b"));

    DynMethods.BoundMethod slashCat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked(new Concatenator("/"));

    Assert.assertEquals("Should use bound object from builder without passing",
        "a/b", slashCat.invoke("a", "b"));
  }

  @Test
  public void testBindStaticMethod() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("cat")
        .impl(Concatenator.class, String[].class);

    TestUtils.assertThrows("Should complain that method is static",
        IllegalStateException.class, () -> builder.buildChecked(new Concatenator()));

    TestUtils.assertThrows("Should complain that method is static",
        IllegalStateException.class, () -> builder.build(new Concatenator()));

    final DynMethods.UnboundMethod staticCat = builder.buildChecked();
    Assert.assertTrue("Should be static", staticCat.isStatic());

    TestUtils.assertThrows("Should complain that method is static",
        IllegalStateException.class, () -> staticCat.bind(new Concatenator()));
  }

  @Test
  public void testStaticMethod() throws Exception {
    DynMethods.StaticMethod staticCat = new DynMethods.Builder("cat")
        .impl(Concatenator.class, String[].class)
        .buildStaticChecked();

    Assert.assertEquals("Should call varargs static method cat(String...)",
        "abcde", staticCat.invokeChecked(
            (Object) new String[] {"a", "b", "c", "d", "e"}));
  }

  @Test
  public void testNonStaticMethod() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String.class, String.class);

    TestUtils.assertThrows("Should complain that method is not static",
        IllegalStateException.class, builder::buildStatic);

    TestUtils.assertThrows("Should complain that method is not static",
        IllegalStateException.class, builder::buildStaticChecked);

    final DynMethods.UnboundMethod cat2 = builder.buildChecked();
    Assert.assertFalse("concat(String,String) should not be static",
        cat2.isStatic());

    TestUtils.assertThrows("Should complain that method is not static",
        IllegalStateException.class, cat2::asStatic);
  }

  @Test
  public void testConstructorImpl() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("newConcatenator")
        .ctorImpl(Concatenator.class, String.class)
        .impl(Concatenator.class, String.class);

    DynMethods.UnboundMethod newConcatenator = builder.buildChecked();
    Assert.assertTrue("Should find constructor implementation",
        newConcatenator instanceof DynConstructors.Ctor);
    Assert.assertTrue("Constructor should be a static method",
        newConcatenator.isStatic());
    Assert.assertFalse("Constructor should not be NOOP",
        newConcatenator.isNoop());

    // constructors cannot be bound
    TestUtils.assertThrows("Should complain that ctor method is static",
        IllegalStateException.class, () -> builder.buildChecked(new Concatenator()));
    TestUtils.assertThrows("Should complain that ctor method is static",
        IllegalStateException.class, () -> builder.build(new Concatenator()));

    Concatenator concatenator = newConcatenator.asStatic().invoke("*");
    Assert.assertEquals("Should function as a concatenator",
        "a*b", concatenator.concat("a", "b"));

    concatenator = newConcatenator.asStatic().invokeChecked("@");
    Assert.assertEquals("Should function as a concatenator",
        "a@b", concatenator.concat("a", "b"));
  }

  @Test
  public void testConstructorImplAfterFactoryMethod() throws Exception {
    DynMethods.UnboundMethod newConcatenator = new DynMethods.Builder("newConcatenator")
        .impl(Concatenator.class, String.class)
        .ctorImpl(Concatenator.class, String.class)
        .buildChecked();

    Assert.assertFalse("Should find factory method before constructor method",
        newConcatenator instanceof DynConstructors.Ctor);
  }

  @Test
  public void testNoop() throws Exception {
    // noop can be unbound, bound, or static
    DynMethods.UnboundMethod noop = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .orNoop()
        .buildChecked();

    Assert.assertTrue("No implementation found, should return NOOP",
        noop.isNoop());
    Assert.assertNull("NOOP should always return null",
        noop.invoke(new Concatenator(), "a"));
    Assert.assertNull("NOOP can be called with null",
        noop.invoke(null, "a"));
    Assert.assertNull("NOOP can be bound",
        noop.bind(new Concatenator()).invoke("a"));
    Assert.assertNull("NOOP can be bound to null",
        noop.bind(null).invoke("a"));
    Assert.assertNull("NOOP can be static",
        noop.asStatic().invoke("a"));
  }
}
