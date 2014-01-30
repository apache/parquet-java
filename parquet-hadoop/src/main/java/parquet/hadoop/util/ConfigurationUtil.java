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
package parquet.hadoop.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.BadConfigurationException;

public class ConfigurationUtil {

  public static Class<?> getClassFromConfig(Configuration configuration, String configName, Class<?> assignableFrom) {
    final String className = configuration.get(configName);
    if (className == null) {
      return null;
    }
    try {
      final Class<?> foundClass = Class.forName(className);
      if (!assignableFrom.isAssignableFrom(foundClass)) {
        throw new BadConfigurationException("class " + className + " set in job conf at "
            + configName + " is not a subclass of " + assignableFrom.getCanonicalName());
      }
      return foundClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("could not instantiate class " + className + " set in job conf at " + configName, e);
    }
  }

  public static Properties toProperties(Configuration configuration) {
    Properties properties = new Properties();
    assert configuration != null;
    Iterator<Map.Entry<String, String>> iter = configuration.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> entry = iter.next();
      properties.put(entry.getKey(), entry.getValue());
    }
    return properties;
  }

}
