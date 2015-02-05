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
package parquet.hive;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

import parquet.hive.internal.Hive010Binding;
import parquet.hive.internal.Hive012Binding;
import parquet.hive.HiveBindingFactory.HiveBindingInstantiationError;
import parquet.hive.HiveBindingFactory.UnexpectedHiveVersionProviderError;

public class TestHiveBindingFactory {
  private HiveBindingFactory hiveBindingFactory;

  @Before
  public void setup() {
    hiveBindingFactory = new HiveBindingFactory();
  }
  @Test
  public void testMissingHiveVersionInfoClass() {
    Assert.assertEquals(Hive010Binding.class, hiveBindingFactory.
        create(new NoopClassLoader()));
  }
  @Test(expected=UnexpectedHiveVersionProviderError.class)
  public void testNoHiveVersion() {
    hiveBindingFactory.createInternal(NoHiveVersion.class);
  }
  @Test
  public void testBlankHiveVersion() {
    hiveBindingFactory.createInternal(BlankHiveVersion.class);
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(BlankHiveVersion.class));
  }
  @Test
  public void testUnknownHiveVersion() {
    hiveBindingFactory.createInternal(BlankHiveVersion.class);
    // returns 0.12 because we don't have hive in classpath
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(BlankHiveVersion.class));
  }
  @Test
  public void testNullHiveVersion() {
    hiveBindingFactory.createInternal(NullHiveVersion.class);
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(NullHiveVersion.class));
  }
  @Test
  public void testHive010() {
    Assert.assertEquals(Hive010Binding.class, hiveBindingFactory.
        createInternal(Hive010Version.class));
  }
  @Test
  public void testHive010WithSpaces() {
    Assert.assertEquals(Hive010Binding.class, hiveBindingFactory.
        createInternal(Hive010VersionWithSpaces.class));
  }
  @Test
  public void testHive011() {
    Assert.assertEquals(Hive010Binding.class, hiveBindingFactory.
        createInternal(Hive011Version.class));
  }
  @Test
  public void testHive012() {
    Assert.assertEquals(Hive012Binding.class, hiveBindingFactory.
        createInternal(Hive012Version.class));
  }
  @Test(expected=HiveBindingInstantiationError.class)
  public void testHive013() {
    hiveBindingFactory.createInternal(Hive013Version.class);
  }

  static class NoopClassLoader extends ClassLoader {
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      throw new ClassNotFoundException(name);
    }    
  }
  static class NoHiveVersion {
    
  }
  static class BlankHiveVersion {
    public static String getVersion() {
      return "";
    } 
  }
  static class UnknownHiveVersion {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_UNKNOWN;
    } 
  }
  static class NullHiveVersion {
    public static String getVersion() {
      return null;
    } 
  }
  static class Hive010Version {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_010;
    } 
  }
  static class Hive010VersionWithSpaces {
    public static String getVersion() {
      return " " + HiveBindingFactory.HIVE_VERSION_010 + " ";
    } 
  }
  static class Hive011Version {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_011;
    } 
  }
  static class Hive012Version {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_012;
    } 
  }
  static class Hive013Version {
    public static String getVersion() {
      return HiveBindingFactory.HIVE_VERSION_013;
    } 
  }
}
