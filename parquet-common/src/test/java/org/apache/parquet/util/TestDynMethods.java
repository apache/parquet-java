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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.parquet.util.Concatenator.SomeCheckedException;
import org.junit.Test;

public class TestDynMethods {
  @Test
  public void testNoImplCall() {
    final DynMethods.Builder builder = new DynMethods.Builder("concat");

    assertThatThrownBy(builder::buildChecked)
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessage("Cannot find method: concat");

    assertThatThrownBy(builder::build)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Cannot find method: concat");
  }

  @Test
  public void testMissingClass() {
    final DynMethods.Builder builder =
        new DynMethods.Builder("concat").impl("not.a.RealClass", String.class, String.class);

    assertThatThrownBy(builder::buildChecked)
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining("Cannot find method: concat");

    assertThatThrownBy(builder::build)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Cannot find method: concat");
  }

  @Test
  public void testMissingMethod() {
    final DynMethods.Builder builder =
        new DynMethods.Builder("concat").impl(Concatenator.class, "cat2strings", String.class, String.class);

    assertThatThrownBy(builder::buildChecked)
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining("Cannot find method: concat");

    assertThatThrownBy(builder::build)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Cannot find method: concat");
  }

  @Test
  public void testFirstImplReturned() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat2 = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .impl(Concatenator.class, String.class, String.class, String.class)
        .buildChecked();

    assertThat((String) cat2.invoke(obj, "a", "b"))
        .as("Should call the 2-arg version successfully")
        .isEqualTo("a-b");

    assertThat((String) cat2.invoke(obj, "a", "b", "c"))
        .as("Should ignore extra arguments")
        .isEqualTo("a-b");

    DynMethods.UnboundMethod cat3 = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .build();

    assertThat((String) cat3.invoke(obj, "a", "b", "c"))
        .as("Should call the 3-arg version successfully")
        .isEqualTo("a-b-c");

    assertThat((String) cat3.invoke(obj, "a", "b"))
        .as("Should call the 3-arg version null padding")
        .isEqualTo("a-b-null");
  }

  @Test
  public void testVarArgs() throws Exception {
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String[].class)
        .buildChecked();

    assertThat((String) cat.invokeChecked(new Concatenator(), (Object) new String[] {"a", "b", "c", "d", "e"}))
        .as("Should use the varargs version")
        .isEqualTo("abcde");

    assertThat((String) cat.bind(new Concatenator()).invokeChecked((Object) new String[] {"a", "b", "c", "d", "e"}))
        .as("Should use the varargs version")
        .isEqualTo("abcde");
  }

  @Test
  public void testIncorrectArguments() throws Exception {
    final Concatenator obj = new Concatenator("-");
    final DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked();

    assertThatThrownBy(() -> cat.invoke(obj, 3, 4))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument type mismatch");

    assertThatThrownBy(() -> cat.invokeChecked(obj, 3, 4))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument type mismatch");
  }

  @Test
  public void testExceptionThrown() throws Exception {
    final SomeCheckedException exc = new SomeCheckedException();
    final Concatenator obj = new Concatenator("-");
    final DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, Exception.class)
        .buildChecked();

    assertThatThrownBy(() -> cat.invokeChecked(obj, exc)).isInstanceOf(SomeCheckedException.class);

    assertThatThrownBy(() -> cat.invoke(obj, exc))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("org.apache.parquet.util.Concatenator$SomeCheckedException");
  }

  @Test
  public void testNameChange() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat = new DynMethods.Builder("cat")
        .impl(Concatenator.class, "concat", String.class, String.class)
        .buildChecked();

    assertThat((String) cat.invoke(obj, "a", "b"))
        .as("Should find 2-arg concat method")
        .isEqualTo("a-b");
  }

  @Test
  public void testStringClassname() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class.getName(), String.class, String.class)
        .buildChecked();

    assertThat((String) cat.invoke(obj, "a", "b"))
        .as("Should find 2-arg concat method")
        .isEqualTo("a-b");
  }

  @Test
  public void testHiddenMethod() throws Exception {
    Concatenator obj = new Concatenator("-");

    assertThatThrownBy(() -> new DynMethods.Builder("setSeparator")
            .impl(Concatenator.class, String.class)
            .buildChecked())
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining("Cannot find method: setSeparator");

    DynMethods.UnboundMethod changeSep = new DynMethods.Builder("setSeparator")
        .hiddenImpl(Concatenator.class, String.class)
        .buildChecked();

    assertThat(changeSep).as("Should find hidden method with hiddenImpl").isNotNull();

    changeSep.invokeChecked(obj, "/");

    assertThat(obj.concat("a", "b"))
        .as("Should use separator / instead of -")
        .isEqualTo("a/b");
  }

  @Test
  public void testBoundMethod() throws Exception {
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked();

    // Unbound methods can be bound multiple times
    DynMethods.BoundMethod dashCat = cat.bind(new Concatenator("-"));
    DynMethods.BoundMethod underCat = cat.bind(new Concatenator("_"));

    assertThat((String) dashCat.invoke("a", "b"))
        .as("Should use '-' object without passing")
        .isEqualTo("a-b");
    assertThat((String) underCat.invoke("a", "b"))
        .as("Should use '_' object without passing")
        .isEqualTo("a_b");

    DynMethods.BoundMethod slashCat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked(new Concatenator("/"));

    assertThat((String) slashCat.invoke("a", "b"))
        .as("Should use bound object from builder without passing")
        .isEqualTo("a/b");
  }

  @Test
  public void testBindStaticMethod() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("cat").impl(Concatenator.class, String[].class);

    assertThatThrownBy(() -> builder.buildChecked(new Concatenator()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot bind static method");

    assertThatThrownBy(() -> builder.build(new Concatenator()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot bind static method");

    final DynMethods.UnboundMethod staticCat = builder.buildChecked();
    assertThat(staticCat.isStatic()).as("Should be static").isTrue();

    assertThatThrownBy(() -> staticCat.bind(new Concatenator()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot bind static method");
  }

  @Test
  public void testStaticMethod() throws Exception {
    DynMethods.StaticMethod staticCat = new DynMethods.Builder("cat")
        .impl(Concatenator.class, String[].class)
        .buildStaticChecked();

    assertThat((String) staticCat.invokeChecked((Object) new String[] {"a", "b", "c", "d", "e"}))
        .as("Should call varargs static method cat(String...)")
        .isEqualTo("abcde");
  }

  @Test
  public void testNonStaticMethod() throws Exception {
    final DynMethods.Builder builder =
        new DynMethods.Builder("concat").impl(Concatenator.class, String.class, String.class);

    assertThatThrownBy(builder::buildStatic)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Method is not static");

    assertThatThrownBy(builder::buildStaticChecked)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Method is not static");

    final DynMethods.UnboundMethod cat2 = builder.buildChecked();
    assertThat(cat2.isStatic())
        .as("concat(String,String) should not be static")
        .isFalse();

    assertThatThrownBy(cat2::asStatic)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Method is not static");
  }

  @Test
  public void testConstructorImpl() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("newConcatenator")
        .ctorImpl(Concatenator.class, String.class)
        .impl(Concatenator.class, String.class);

    DynMethods.UnboundMethod newConcatenator = builder.buildChecked();
    assertThat(newConcatenator)
        .as("Should find constructor implementation")
        .isInstanceOf(DynConstructors.Ctor.class);
    assertThat(newConcatenator.isStatic())
        .as("Constructor should be a static method")
        .isTrue();
    assertThat(newConcatenator.isNoop())
        .as("Constructor should not be NOOP")
        .isFalse();

    // constructors cannot be bound
    assertThatThrownBy(() -> builder.buildChecked(new Concatenator()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot bind constructors");
    assertThatThrownBy(() -> builder.build(new Concatenator()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot bind constructors");

    Concatenator concatenator = newConcatenator.asStatic().invoke("*");
    assertThat(concatenator.concat("a", "b"))
        .as("Should function as a concatenator")
        .isEqualTo("a*b");

    concatenator = newConcatenator.asStatic().invokeChecked("@");
    assertThat(concatenator.concat("a", "b"))
        .as("Should function as a concatenator")
        .isEqualTo("a@b");
  }

  @Test
  public void testConstructorImplAfterFactoryMethod() throws Exception {
    DynMethods.UnboundMethod newConcatenator = new DynMethods.Builder("newConcatenator")
        .impl(Concatenator.class, String.class)
        .ctorImpl(Concatenator.class, String.class)
        .buildChecked();

    assertThat(newConcatenator)
        .as("Should find factory method before constructor method")
        .isNotInstanceOf(DynConstructors.Ctor.class);
  }

  @Test
  public void testNoop() throws Exception {
    // noop can be unbound, bound, or static
    DynMethods.UnboundMethod noop = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .orNoop()
        .buildChecked();

    assertThat(noop.isNoop())
        .as("No implementation found, should return NOOP")
        .isTrue();
    assertThat((Object) noop.invoke(new Concatenator(), "a"))
        .as("NOOP should always return null")
        .isNull();
    assertThat((Object) noop.invoke(null, "a"))
        .as("NOOP can be called with null")
        .isNull();
    assertThat((Object) noop.bind(new Concatenator()).invoke("a"))
        .as("NOOP can be bound")
        .isNull();
    assertThat((Object) noop.bind(null).invoke("a"))
        .as("NOOP can be bound to null")
        .isNull();
    assertThat((Object) noop.asStatic().invoke("a"))
        .as("NOOP can be static")
        .isNull();
  }

  static class Available {
    public static String register() {
      return "available";
    }

    @SuppressWarnings("unused")
    private static String hiddenRegister() {
      return "hidden-available";
    }
  }

  @Test
  public void implWithNoClassDefFoundError() throws NoSuchMethodException {
    ClassLoader errorLoader = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
      @Override
      public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if ("org.apache.parquet.MissingDependencyClass".equals(name)) {
          throw new NoClassDefFoundError("some/TransitiveDependency");
        }

        return super.loadClass(name, resolve);
      }
    };

    assertThatThrownBy(() -> new DynMethods.Builder("register")
            .loader(errorLoader)
            .impl("org.apache.parquet.MissingDependencyClass")
            .buildStaticChecked())
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessage("Cannot find method: register");

    assertThat(new DynMethods.Builder("register")
            .loader(errorLoader)
            .impl("org.apache.parquet.MissingDependencyClass")
            .impl(Available.class)
            .buildStaticChecked()
            .<String>invoke())
        .isEqualTo("available");

    assertThat(new DynMethods.Builder("register")
            .loader(errorLoader)
            .hiddenImpl("org.apache.parquet.MissingDependencyClass")
            .hiddenImpl(Available.class, "hiddenRegister")
            .buildStaticChecked()
            .<String>invoke())
        .isEqualTo("hidden-available");
  }

  @Test
  public void implWithExceptionInInitializerError() {
    ClassLoader errorLoader = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
      @Override
      public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        throw new ExceptionInInitializerError("static initializer failed");
      }
    };

    assertThatThrownBy(() -> new DynMethods.Builder("register")
            .loader(errorLoader)
            .impl("org.apache.parquet.FailingInitClass")
            .buildStaticChecked())
        .isInstanceOf(ExceptionInInitializerError.class)
        .hasMessage("static initializer failed");
  }
}
