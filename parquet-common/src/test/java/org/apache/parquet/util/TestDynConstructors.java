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

public class TestDynConstructors {
  @Test
  public void testNoImplCall() {
    final DynConstructors.Builder builder = new DynConstructors.Builder();

    assertThatThrownBy(builder::buildChecked)
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining("Cannot find constructor for null");

    assertThatThrownBy(builder::build)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Cannot find constructor for null");
  }

  @Test
  public void testMissingClass() {
    final DynConstructors.Builder builder = new DynConstructors.Builder().impl("not.a.RealClass");

    assertThatThrownBy(builder::buildChecked)
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining("Cannot find constructor for null");

    assertThatThrownBy(builder::build)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Cannot find constructor for null");
  }

  @Test
  public void testMissingConstructor() {
    final DynConstructors.Builder builder =
        new DynConstructors.Builder().impl(Concatenator.class, String.class, String.class);

    assertThatThrownBy(builder::buildChecked)
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining(
            "Missing org.apache.parquet.util.Concatenator(java.lang.String,java.lang.String)");

    assertThatThrownBy(builder::build)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Missing org.apache.parquet.util.Concatenator(java.lang.String,java.lang.String)");
  }

  @Test
  public void testFirstImplReturned() throws Exception {
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class, String.class)
        .impl(Concatenator.class)
        .buildChecked();

    Concatenator dashCat = sepCtor.newInstanceChecked("-");
    assertThat(dashCat.concat("a", "b"))
        .as("Should construct with the 1-arg version")
        .isEqualTo("a-b");

    assertThatThrownBy(() -> sepCtor.newInstanceChecked("/", "-"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("wrong number of arguments");

    assertThatThrownBy(() -> sepCtor.newInstance("/", "-"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("wrong number of arguments");

    DynConstructors.Ctor<Concatenator> defaultCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class)
        .impl(Concatenator.class, String.class)
        .buildChecked();

    Concatenator cat = defaultCtor.newInstanceChecked();
    assertThat(cat.concat("a", "b"))
        .as("Should construct with the no-arg version")
        .isEqualTo("ab");
  }

  @Test
  public void testExceptionThrown() throws Exception {
    final SomeCheckedException exc = new SomeCheckedException();
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class, Exception.class)
        .buildChecked();

    assertThatThrownBy(() -> sepCtor.newInstanceChecked(exc)).isInstanceOf(SomeCheckedException.class);

    assertThatThrownBy(() -> sepCtor.newInstance(exc))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("org.apache.parquet.util.Concatenator$SomeCheckedException");
  }

  @Test
  public void testStringClassname() throws Exception {
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl(Concatenator.class.getName(), String.class)
        .buildChecked();

    assertThat(sepCtor.newInstance("-")).as("Should find 1-arg constructor").isNotNull();
  }

  @Test
  public void testHiddenMethod() throws Exception {
    assertThatThrownBy(() -> new DynMethods.Builder("setSeparator")
            .impl(Concatenator.class, char.class)
            .buildChecked())
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining("Cannot find method: setSeparator");

    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .hiddenImpl(Concatenator.class.getName(), char.class)
        .buildChecked();

    assertThat(sepCtor).as("Should find hidden ctor with hiddenImpl").isNotNull();

    Concatenator slashCat = sepCtor.newInstanceChecked('/');

    assertThat(slashCat.concat("a", "b")).as("Should use separator /").isEqualTo("a/b");
  }

  @Test
  public void testBind() throws Exception {
    final DynConstructors.Ctor<Concatenator> ctor =
        new DynConstructors.Builder().impl(Concatenator.class.getName()).buildChecked();

    assertThat(ctor.isStatic()).as("Should always be static").isTrue();

    assertThatThrownBy(() -> ctor.bind(null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot bind constructors");
  }

  @Test
  public void testInvoke() throws Exception {
    final DynMethods.UnboundMethod ctor =
        new DynConstructors.Builder().impl(Concatenator.class.getName()).buildChecked();

    assertThatThrownBy(() -> ctor.invokeChecked("a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid call to constructor: target must be null");

    assertThatThrownBy(() -> ctor.invoke("a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid call to constructor: target must be null");

    assertThat((Object) ctor.invokeChecked(null))
        .as("Should allow invokeChecked(null, ...)")
        .isNotNull();
    assertThat((Object) ctor.invoke(null))
        .as("Should allow invoke(null, ...)")
        .isNotNull();
  }

  @Test
  public void implWithNoClassDefFoundError() throws Exception {
    ClassLoader errorLoader = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
      @Override
      public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if ("org.apache.parquet.MissingDependencyClass".equals(name)) {
          throw new NoClassDefFoundError("some/TransitiveDependency");
        }

        return super.loadClass(name, resolve);
      }
    };

    assertThatThrownBy(() -> new DynConstructors.Builder(MyInterface.class)
            .loader(errorLoader)
            .impl("org.apache.parquet.MissingDependencyClass")
            .buildChecked())
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageStartingWith("Cannot find constructor for interface")
        .hasMessageContaining("Missing org.apache.parquet.MissingDependencyClass");

    assertThat(new DynConstructors.Builder(MyInterface.class)
            .loader(errorLoader)
            .impl("org.apache.parquet.MissingDependencyClass")
            .impl(MyClass.class)
            .buildChecked()
            .newInstance())
        .isInstanceOf(MyClass.class);

    assertThat(new DynConstructors.Builder(MyInterface.class)
            .loader(errorLoader)
            .hiddenImpl("org.apache.parquet.MissingDependencyClass")
            .impl(MyClass.class)
            .buildChecked()
            .newInstance())
        .isInstanceOf(MyClass.class);
  }

  @Test
  public void implWithExceptionInInitializerError() {
    ClassLoader errorLoader = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
      @Override
      public Class<?> loadClass(String name, boolean resolve) {
        throw new ExceptionInInitializerError("static initializer failed");
      }
    };

    assertThatThrownBy(() -> new DynConstructors.Builder(MyInterface.class)
            .loader(errorLoader)
            .impl("org.apache.parquet.FailingInitClass")
            .buildChecked())
        .isInstanceOf(ExceptionInInitializerError.class)
        .hasMessage("static initializer failed");
  }

  public interface MyInterface {}

  public static class MyClass implements MyInterface {}
}
