/**
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
package parquet.hive;

import java.lang.reflect.Method;

import parquet.hive.internal.Hive010Binding;
import parquet.hive.internal.Hive012Binding;

public class HiveBindingFactory {
  private static final String HIVE_VERSION_CLASS_NAME = "org.apache.hive.common.util.HiveVersionInfo";
  private static final String HIVE_VERSION_METHOD_NAME = "getVersion";
  private static final String HIVE_VERSION_NULL = "<null>";
  static final String HIVE_VERSION_010 = "0.10";
  static final String HIVE_VERSION_011 = "0.11";
  static final String HIVE_VERSION_012 = "0.12";
  static final String HIVE_VERSION_013 = "0.13";

  public HiveBinding create() {
    Class<? extends HiveBinding> bindingClazz = create(HiveBindingFactory.class
        .getClassLoader());
    try {
      return bindingClazz.newInstance();
    } catch (Exception e) {
      throw new HiveBindingInstantiationError("Unexpected error creating instance"
          + " of " + bindingClazz.getCanonicalName(), e);
    }
  }

  @SuppressWarnings("rawtypes")
  Class<? extends HiveBinding> create(ClassLoader classLoader) {
    // HiveVersionInfo was added in 0.11, if the class does
    // not exist then return the hive binding for 0.10
    Class hiveVersionInfo;
    try {
      hiveVersionInfo = Class.forName(HIVE_VERSION_CLASS_NAME, true, classLoader);
    } catch (ClassNotFoundException e) {
      return Hive010Binding.class;
    }
    return createInternal(hiveVersionInfo);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  Class<? extends HiveBinding> createInternal(Class hiveVersionInfo) {
    String hiveVersion;
    try {
      Method getVersionMethod = hiveVersionInfo.
          getMethod(HIVE_VERSION_METHOD_NAME, (Class[])null);
      hiveVersion = trimVersion((String)getVersionMethod.
          invoke(null, (Object[])null));
    } catch (Exception e) {
      throw new UnexpectedHiveVersionProviderError("Unexpected error whilst " +
          "determining Hive version", e);
    }
    if(hiveVersion.startsWith(HIVE_VERSION_010)) {
      return Hive010Binding.class;
    } else if(hiveVersion.startsWith(HIVE_VERSION_011)) {
      // 0.10 binding should work with 0.11
      return Hive010Binding.class;
    } else if(hiveVersion.startsWith(HIVE_VERSION_012)) {
      return Hive012Binding.class;
    } else if(hiveVersion.startsWith(HIVE_VERSION_013)) {
      // 11/26/2013: it looks like the 0.12 binding will work for 0.13
      return Hive012Binding.class;
    }
    throw new UnknownHiveVersionError("Unknown Hive version '" + hiveVersion + "'");    
  }
  
  private static String trimVersion(String s) {
    if(s == null) {
      return HIVE_VERSION_NULL;
    }
    return s.trim();
  }
  static class HiveBindingInstantiationError extends Error {
    private static final long serialVersionUID = -7344858060142118L;
    public HiveBindingInstantiationError(String msg, Exception e) {
      super(msg, e);
    }
  }
  static class UnexpectedHiveVersionProviderError extends Error {
    private static final long serialVersionUID = -7344858060142118L;
    public UnexpectedHiveVersionProviderError(String msg, Exception e) {
      super(msg, e);
    }
  }
  static class UnknownHiveVersionError extends Error {
    private static final long serialVersionUID = -6736485780607642118L;
    public UnknownHiveVersionError(String msg) {
      super(msg);
    }
  }
}
