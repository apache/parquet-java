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

import parquet.Log;
import parquet.hive.internal.Hive010Binding;
import parquet.hive.internal.Hive012Binding;

/**
 * Factory for creating HiveBinding objects based on the version of Hive
 * available in the classpath. This class does not provide static methods
 * to enable mocking.
 */
public class HiveBindingFactory {
  private static final Log LOG = Log.getLog(HiveBindingFactory.class);
  private static final String HIVE_VERSION_CLASS_NAME = "org.apache.hive.common.util.HiveVersionInfo";
  private static final String HIVE_VERSION_METHOD_NAME = "getVersion";
  private static final String HIVE_VERSION_NULL = "<null>";
  static final String HIVE_VERSION_010 = "0.10";
  static final String HIVE_VERSION_011 = "0.11";
  static final String HIVE_VERSION_012 = "0.12";
  static final String HIVE_VERSION_013 = "0.13";

  /**
   * @return HiveBinding based on the Hive version in the classpath
   */
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

  /**
   * Internal method visible for testing purposes
   */
  @SuppressWarnings("rawtypes")
  Class<? extends HiveBinding> create(ClassLoader classLoader) {
    // HiveVersionInfo was added in 0.11, if the class does
    // not exist then return the hive binding for 0.10
    Class hiveVersionInfo;
    try {
      hiveVersionInfo = Class.forName(HIVE_VERSION_CLASS_NAME, true, classLoader);
    } catch (ClassNotFoundException e) {
      LOG.debug("Class " + HIVE_VERSION_CLASS_NAME + ", not found, returning " + 
          Hive010Binding.class.getSimpleName());
      return Hive010Binding.class;
    }
    return createInternal(hiveVersionInfo);
  }

  /**
   * Internal method visible for testing purposes
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  Class<? extends HiveBinding> createInternal(Class hiveVersionInfo) {
    String hiveVersion;
    try {
      Method getVersionMethod = hiveVersionInfo.
          getMethod(HIVE_VERSION_METHOD_NAME, (Class[])null);
      String rawVersion = (String)getVersionMethod.invoke(null, (Object[])null);
      LOG.debug("Raw Version from " + hiveVersionInfo.getSimpleName() + " is '" +
          rawVersion + "'");
      hiveVersion = trimVersion(rawVersion);
    } catch (Exception e) {
      throw new UnexpectedHiveVersionProviderError("Unexpected error whilst " +
          "determining Hive version", e);
    }
    if(hiveVersion.startsWith(HIVE_VERSION_010)) {
      LOG.debug("Hive version " + hiveVersion + ", returning " +
          Hive010Binding.class.getSimpleName());
      return Hive010Binding.class;
    } else if(hiveVersion.startsWith(HIVE_VERSION_011)) {
      LOG.debug("Hive version " + hiveVersion + ", returning " +
          Hive010Binding.class.getSimpleName() + " as it's expected the 0.10 " +
          "binding will work with 0.11");
      return Hive010Binding.class;
    }
    LOG.debug("Hive version " + hiveVersion + ", returning " +
        Hive012Binding.class.getSimpleName());
    // as of 11/26/2013 it looks like the 0.12 binding will work for 0.13
    return Hive012Binding.class;
  }
  
  private static String trimVersion(String s) {
    if(s == null) {
      return HIVE_VERSION_NULL;
    }
    return s.trim();
  }
  static class HiveBindingInstantiationError extends Error {
    private static final long serialVersionUID = -9348060142128L;
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
}
