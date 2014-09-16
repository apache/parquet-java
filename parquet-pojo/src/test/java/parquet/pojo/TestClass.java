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

import java.util.Arrays;

public class TestClass {
  private boolean bool;
  private Boolean boolRef;
  private byte b;
  private Byte bref;
  private char c;
  private Character cref;
  private short s;
  private Short sref;
  private int i;
  private Integer iref;
  private long l;
  private Long lref;
  private float f;
  private Float fref;
  private double d;
  private Double dref;
  private TestEnum testEnum;
  private String str;
  private byte[] bytes;

  public TestClass() {

  }

  public TestClass(
    boolean bool, Boolean boolRef,
    byte b, Byte bref, char c, Character cref, short s, Short sref, int i, Integer iref, long l, Long lref, float f,
    Float fref, double d, Double dref, TestEnum testEnum, String str, byte[] bytes
  ) {
    this.bool = bool;
    this.boolRef = boolRef;
    this.b = b;
    this.bref = bref;
    this.c = c;
    this.cref = cref;
    this.s = s;
    this.sref = sref;
    this.i = i;
    this.iref = iref;
    this.l = l;
    this.lref = lref;
    this.f = f;
    this.fref = fref;
    this.d = d;
    this.dref = dref;
    this.testEnum = testEnum;
    this.str = str;
    this.bytes = bytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestClass testClass = (TestClass) o;

    if (b != testClass.b) {
      return false;
    }
    if (bool != testClass.bool) {
      return false;
    }
    if (c != testClass.c) {
      return false;
    }
    if (Double.compare(testClass.d, d) != 0) {
      return false;
    }
    if (Float.compare(testClass.f, f) != 0) {
      return false;
    }
    if (i != testClass.i) {
      return false;
    }
    if (l != testClass.l) {
      return false;
    }
    if (s != testClass.s) {
      return false;
    }
    if (boolRef != null ? !boolRef.equals(testClass.boolRef) : testClass.boolRef != null) {
      return false;
    }
    if (bref != null ? !bref.equals(testClass.bref) : testClass.bref != null) {
      return false;
    }
    if (!Arrays.equals(bytes, testClass.bytes)) {
      return false;
    }
    if (cref != null ? !cref.equals(testClass.cref) : testClass.cref != null) {
      return false;
    }
    if (dref != null ? !dref.equals(testClass.dref) : testClass.dref != null) {
      return false;
    }
    if (fref != null ? !fref.equals(testClass.fref) : testClass.fref != null) {
      return false;
    }
    if (iref != null ? !iref.equals(testClass.iref) : testClass.iref != null) {
      return false;
    }
    if (lref != null ? !lref.equals(testClass.lref) : testClass.lref != null) {
      return false;
    }
    if (sref != null ? !sref.equals(testClass.sref) : testClass.sref != null) {
      return false;
    }
    if (str != null ? !str.equals(testClass.str) : testClass.str != null) {
      return false;
    }
    if (testEnum != testClass.testEnum) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = (bool ? 1 : 0);
    result = 31 * result + (boolRef != null ? boolRef.hashCode() : 0);
    result = 31 * result + (int) b;
    result = 31 * result + (bref != null ? bref.hashCode() : 0);
    result = 31 * result + (int) c;
    result = 31 * result + (cref != null ? cref.hashCode() : 0);
    result = 31 * result + (int) s;
    result = 31 * result + (sref != null ? sref.hashCode() : 0);
    result = 31 * result + i;
    result = 31 * result + (iref != null ? iref.hashCode() : 0);
    result = 31 * result + (int) (l ^ (l >>> 32));
    result = 31 * result + (lref != null ? lref.hashCode() : 0);
    result = 31 * result + (f != +0.0f ? Float.floatToIntBits(f) : 0);
    result = 31 * result + (fref != null ? fref.hashCode() : 0);
    temp = Double.doubleToLongBits(d);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + (dref != null ? dref.hashCode() : 0);
    result = 31 * result + (testEnum != null ? testEnum.hashCode() : 0);
    result = 31 * result + (str != null ? str.hashCode() : 0);
    result = 31 * result + (bytes != null ? Arrays.hashCode(bytes) : 0);
    return result;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public boolean isBool() {
    return bool;
  }

  public void setBool(boolean bool) {
    this.bool = bool;
  }

  public Boolean getBoolRef() {
    return boolRef;
  }

  public void setBoolRef(Boolean boolRef) {
    this.boolRef = boolRef;
  }

  public TestEnum getTestEnum() {
    return testEnum;
  }

  public void setTestEnum(TestEnum testEnum) {
    this.testEnum = testEnum;
  }

  public byte getB() {
    return b;
  }

  public void setB(byte b) {
    this.b = b;
  }

  public Byte getBref() {
    return bref;
  }

  public void setBref(Byte bref) {
    this.bref = bref;
  }

  public char getC() {
    return c;
  }

  public void setC(char c) {
    this.c = c;
  }

  public Character getCref() {
    return cref;
  }

  public void setCref(Character cref) {
    this.cref = cref;
  }

  public short getS() {
    return s;
  }

  public void setS(short s) {
    this.s = s;
  }

  public Short getSref() {
    return sref;
  }

  public void setSref(Short sref) {
    this.sref = sref;
  }

  public int getI() {
    return i;
  }

  public void setI(int i) {
    this.i = i;
  }

  public Integer getIref() {
    return iref;
  }

  public void setIref(Integer iref) {
    this.iref = iref;
  }

  public long getL() {
    return l;
  }

  public void setL(long l) {
    this.l = l;
  }

  public Long getLref() {
    return lref;
  }

  public void setLref(Long lref) {
    this.lref = lref;
  }

  public float getF() {
    return f;
  }

  public void setF(float f) {
    this.f = f;
  }

  public Float getFref() {
    return fref;
  }

  public void setFref(Float fref) {
    this.fref = fref;
  }

  public double getD() {
    return d;
  }

  public void setD(double d) {
    this.d = d;
  }

  public Double getDref() {
    return dref;
  }

  public void setDref(Double dref) {
    this.dref = dref;
  }

  @Override
  public String toString() {
    return "TestClass{" +
      "bool=" + bool +
      ", boolRef=" + boolRef +
      ", b=" + b +
      ", bref=" + bref +
      ", c=" + c +
      ", cref=" + cref +
      ", s=" + s +
      ", sref=" + sref +
      ", i=" + i +
      ", iref=" + iref +
      ", l=" + l +
      ", lref=" + lref +
      ", f=" + f +
      ", fref=" + fref +
      ", d=" + d +
      ", dref=" + dref +
      ", testEnum=" + testEnum +
      ", str='" + str + '\'' +
      ", bytes=" + Arrays.toString(bytes) +
      '}';
  }
}
