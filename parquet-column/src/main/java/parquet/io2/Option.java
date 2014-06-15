/**
 * Copyright 2014 GoDaddy, Inc.
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
package parquet.io2;

public abstract class Option<T> {
  Option() {
  }

  private static class Some<T> extends Option<T> {
    private final T value;

    public Some(final T value) {
      this.value = value;
    }

    @Override
    public <R> Option<R> map(final Function<T, R> f) {
      return new Some<R>(f.call(this.value));
    }

    @Override
    public <R> Option<R> flatMap(final Function<T, Option<R>> f) {
      return f.call(this.value);
    }

    @Override
    public Option<T> or(final Function<Void, Option<T>> alternative) {
      return this;
    }

    @Override
    public <R> Option<R> ap(final Option<Function<T, R>> f) {
      return f.map(new Function<Function<T, R>, R>() {
        @Override
        public R call(final Function<T, R> g) {
          return g.call(value);
        }
      });
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public String toString() {
      return "Some{" +
          "value=" + value +
          '}';
    }
  }

  private final static class None<T> extends Option<T> {
    public None() {
    }

    @Override
    public <R> Option<R> map(final Function<T, R> f) {
      return new None<R>();
    }

    @Override
    public <R> Option<R> flatMap(final Function<T, Option<R>> f) {
      return new None<R>();
    }

    @Override
    public Option<T> or(final Function<Void, Option<T>> alternative) {
      return alternative.call(null);
    }

    @Override
    public <R> Option<R> ap(final Option<Function<T, R>> f) {
      return new None<R>();
    }

    @Override
    public T get() {
      throw new RuntimeException();
    }

    @Override
    public String toString() {
      return "None";
    }
  }

  public abstract <R> Option<R> map(final Function<T, R> f);
  public abstract <R> Option<R> flatMap(final Function<T, Option<R>> f);
  public abstract Option<T> or(final Function<Void, Option<T>> alternative);
  public abstract <R> Option<R> ap(final Option<Function<T, R>> f);
  public abstract T get();

  public static <T> Option<T> pure(final T value) {
    return new Some<T>(value);
  }

  public static <T> Option<T> empty() {
    return new None<T>();
  }
}
