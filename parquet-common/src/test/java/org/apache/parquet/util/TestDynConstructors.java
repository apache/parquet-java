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

public class TestDynConstructors {
  @Test
  public void testNoImplCall() {
    final DynConstructors.Builder builder = new DynConstructors.Builder();

    TestUtils.assertThrows("Checked build should throw NoSuchMethodException",
        NoSuchMethodException.class, new Callable() {
          @Override
          public Object call() throws NoSuchMethodException {
            return builder.buildChecked();
          }
        });

    TestUtils.assertThrows("Normal build should throw RuntimeException",
        RuntimeException.class, new Runnable() {
          @Override
          public void run() {
            builder.build();
          }
        });
  }

  @Test
  public void testMissingClass() {
    final DynConstructors.Builder builder = new DynConstructors.Builder()
        .impl("not.a.RealClass");

    TestUtils.assertThrows("Checked build should throw NoSuchMethodException",
        NoSuchMethodException.class, new Callable() {
          @Override
          public Object call() throws NoSuchMethodException {
            return builder.buildChecked();
          }
        });

    TestUtils.assertThrows("Normal build should throw RuntimeException",
        RuntimeException.class, new Runnable() {
          @Override
          public void run() {
            builder.build();
          }
        });
  }

  @Test
  public void testMissingConstructor() {
    final DynConstructors.Builder builder = new DynConstructors.Builder()
        .impl(Concatenator.class, String.class, String.class);

    TestUtils.assertThrows("Checked build should throw NoSuchMethodException",
        NoSuchMethodException.class, new Callable() {
          @Override
          public Object call() throws NoSuchMethodException {
            return builder.buildChecked();
          }
        });

    TestUtils.assertThrows("Normal build should throw RuntimeException",
        RuntimeException.class, new Runnable() {
          @Override
          public void run() {
            builder.build();
          }
        });
  }

  @Test
  public void testFirstImplReturned() throws Exception {
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class, String.class)
        .impl(Concatenator.class)
        .buildChecked();

    Concatenator dashCat = sepCtor.newInstanceChecked("-");
    Assert.assertEquals("Should construct with the 1-arg version",
        "a-b", dashCat.concat("a", "b"));

    TestUtils.assertThrows("Should complain about extra arguments",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            return sepCtor.newInstanceChecked("/", "-");
          }
        });

    TestUtils.assertThrows("Should complain about extra arguments",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            return sepCtor.newInstance("/", "-");
          }
        });

    DynConstructors.Ctor<Concatenator> defaultCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class)
        .impl(Concatenator.class, String.class)
        .buildChecked();

    Concatenator cat = defaultCtor.newInstanceChecked();
    Assert.assertEquals("Should construct with the no-arg version",
        "ab", cat.concat("a", "b"));
  }

  @Test
  public void testExceptionThrown() throws Exception {
    final SomeCheckedException exc = new SomeCheckedException();
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class, Exception.class)
        .buildChecked();

    TestUtils.assertThrows("Should re-throw the exception",
        SomeCheckedException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            return sepCtor.newInstanceChecked(exc);
          }
        });

    TestUtils.assertThrows("Should wrap the exception in RuntimeException",
        RuntimeException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            return sepCtor.newInstance(exc);
          }
        });
  }

  @Test
  public void testStringClassname() throws Exception {
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl(Concatenator.class.getName(), String.class)
        .buildChecked();

    Assert.assertNotNull("Should find 1-arg constructor", sepCtor.newInstance("-"));
  }

  @Test
  public void testHiddenMethod() throws Exception {
    TestUtils.assertThrows("Should fail to find hidden method",
        NoSuchMethodException.class, new Callable() {
          @Override
          public Object call() throws NoSuchMethodException {
            return new DynMethods.Builder("setSeparator")
                .impl(Concatenator.class, char.class)
                .buildChecked();
          }
        });

    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .hiddenImpl(Concatenator.class.getName(), char.class)
        .buildChecked();

    Assert.assertNotNull("Should find hidden ctor with hiddenImpl", sepCtor);

    Concatenator slashCat = sepCtor.newInstanceChecked('/');

    Assert.assertEquals("Should use separator /",
        "a/b", slashCat.concat("a", "b"));
  }

  @Test
  public void testBind() throws Exception {
    final DynConstructors.Ctor<Concatenator> ctor = new DynConstructors.Builder()
        .impl(Concatenator.class.getName())
        .buildChecked();

    Assert.assertTrue("Should always be static", ctor.isStatic());

    TestUtils.assertThrows("Should complain that method is static",
        IllegalStateException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            return ctor.bind(null);
          }
        });
  }

  @Test
  public void testInvoke() throws Exception {
    final DynMethods.UnboundMethod ctor = new DynConstructors.Builder()
        .impl(Concatenator.class.getName())
        .buildChecked();

    TestUtils.assertThrows("Should complain that target must be null",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            return ctor.invokeChecked("a");
          }
        });

    TestUtils.assertThrows("Should complain that target must be null",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            return ctor.invoke("a");
          }
        });

    Assert.assertNotNull("Should allow invokeChecked(null, ...)",
        ctor.invokeChecked(null));
    Assert.assertNotNull("Should allow invoke(null, ...)",
        ctor.invoke(null));
  }
}
