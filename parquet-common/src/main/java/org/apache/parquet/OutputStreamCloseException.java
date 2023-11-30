/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet;

/**
 * Runtime exception indicating that a stream failed to be closed properly.
 * <p>
 * Used to wrap up the checked IOException usually thrown from IO operations,
 * these are generally not recoverable so it does not make sense to pollute the
 * codebase declaring that they can be thrown whenever resources are being
 * closed out.
 */
public class OutputStreamCloseException extends ParquetRuntimeException {

  private static final long serialVersionUID = 1L;

  public OutputStreamCloseException() {}

  public OutputStreamCloseException(String message, Throwable cause) {
    super(message, cause);
  }

  public OutputStreamCloseException(String message) {
    super(message);
  }

  public OutputStreamCloseException(Throwable cause) {
    super(cause);
  }
}
