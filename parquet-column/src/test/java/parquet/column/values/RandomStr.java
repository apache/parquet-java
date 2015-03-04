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
package parquet.column.values;

import java.util.Random;

/**
 * 
 * @author Aniket Mokashi
 *
 */
public class RandomStr {
  private final char[] alphanumeric=alphanumeric();
  private final Random rand;

  public RandomStr(){this(null);}

  public RandomStr(Random rand){
    this.rand=(rand!=null) ? rand : new Random();
  }

  public String get(int len){
    StringBuffer out=new StringBuffer();

    while(out.length() < len){
      int idx=Math.abs(( rand.nextInt() % alphanumeric.length ));
      out.append(alphanumeric[idx]);
    }
    return out.toString();
  }

  // create alphanumeric char array
  private char[] alphanumeric(){
    StringBuffer buf=new StringBuffer(128);
    for(int i=48; i<= 57;i++)buf.append((char)i); // 0-9
    for(int i=65; i<= 90;i++)buf.append((char)i); // A-Z
    for(int i=97; i<=122;i++)buf.append((char)i); // a-z
    return buf.toString().toCharArray();
  }
}