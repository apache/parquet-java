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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Utility annotation for use on fields when an interface is desired on the left side of a field declaration.
 * Usage:
 * <p/>
 * <pre>
 * {@code
 * public SomeClass {
 *     &#064ConcreteType(createAs=HashMap.class)
 *     private Map<Integer, Integer> intMap = new HashMap<Integer, Integer>
 * }
 * }
 * </pre>
 *
 * @author Jason Ruckman https://github.com/JasonRuckman
 */
@Target(value = ElementType.FIELD)
@Retention(value = RetentionPolicy.RUNTIME)
public @interface ConcreteType {
  Class createAs();
}
