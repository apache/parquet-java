<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Migrating tests (JUnit 4 → JUnit 5 + AssertJ)

Process for **converting existing JUnit 4 tests** to JUnit 5 + AssertJ. The
**target style** for the migrated code is defined in
[`writing-tests.md`](./writing-tests.md) — read that first. This document adds
the JUnit → AssertJ mapping, guidance for a bulk first pass, and the
post-migration audit that catches what a bulk `assertEquals` → `assertThat`
rewrite leaves behind.

## JUnit → AssertJ mapping

| JUnit | AssertJ |
|-------|---------|
| `assertEquals(expected, actual)` | `assertThat(actual).isEqualTo(expected)` |
| `assertSame(expected, actual)` | `assertThat(actual).isSameAs(expected)` |
| `assertArrayEquals(expected, actual)` | `assertThat(actual).isEqualTo(expected)` or `containsExactly(...)` for primitives |
| `assertEquals(n, collection.size())` | `assertThat(collection).hasSize(n)` |
| `assertEquals(collection.size(), other.size())` | `assertThat(collection).hasSameSizeAs(other)` |
| `assertTrue(collection.contains(x))` | `assertThat(collection).contains(x)` |
| `assertFalse(collection.contains(x))` | `assertThat(collection).doesNotContain(x)` |
| `assertTrue(collection.isEmpty())` | `assertThat(collection).isEmpty()` |
| `assertFalse(collection.isEmpty())` | `assertThat(collection).isNotEmpty()` |
| `assertEquals(0, command.run())` | `assertThat(command.run()).isZero()` |
| `assertEquals(0, file.length())` | `assertThat(file.length()).isZero()` |
| `assertTrue(0 < file.length())` | `assertThat(file.length()).isPositive()` |
| `assertTrue(file.exists())` | `assertThat(file).exists()` |
| `@Test(expected = X.class)` / `assertThrows(X.class, …)` | `assertThatThrownBy(…).isInstanceOf(X.class).hasMessage(…)` |
| `assertTrue(condition)` | `assertThat(condition).isTrue()` |
| `assertNull(x)` | `assertThat(x).isNull()` |
| `assertTrue(true)` after successful run | `assertThatCode(() -> action()).doesNotThrowAnyException()` |
| `assert (exitCode == 0)` | `assertThat(exitCode).isZero()` |
| `Assume.assumeTrue(condition)` (JUnit 4) | `assumeThat(condition).isTrue()` (AssertJ) |
| `Assumptions.assumeTrue(condition)` (JUnit 5) | `assumeThat(condition).isTrue()` (AssertJ) |
| `assertEquals(Collections.emptyList(), list)` | `assertThat(list).isEmpty()` |
| `assertEquals(List.of(a, b, …), list)` | `assertThat(list).containsExactly(a, b, …)` |
| `assertEquals(otherList, list)` (ordered) | `assertThat(list).containsExactlyElementsOf(otherList)` |
| `TestUtils.assertThrows(…)` | `assertThatThrownBy(…).isInstanceOf(…).hasMessage(…)` |
| try/catch + `fail` for expected exception | `assertThatThrownBy(…)` or `assertThatCode(…).doesNotThrowAnyException()` |

See [`writing-tests.md`](./writing-tests.md) for the full rationale behind each
target form (actual-vs-expected order, collection/optional/iterator assertions,
`@TempDir`, assumptions, and exception style).

## Migrating custom `assertThrows` helpers

Many legacy tests used helpers like `assertThrows(String description, Class<? extends Exception> expected, Runnable r)` that only verified the **exception class**. The first `String` parameter was a **test description** (shown when no exception was thrown), **not** `exception.getMessage()`.

When converting these helpers:

1. Replace with `assertThatThrownBy` + `isInstanceOf`.
2. **Also** add `hasMessage` / `hasMessageContaining` using the **actual** message from the production `throw` statement or a test run — do **not** copy the old description string into `hasMessage`.

```java
// Legacy helper — description is NOT the exception message
assertThrows(
    "Should not allow TIME_MICROS with " + primitive,
    IllegalArgumentException.class,
    () -> new AvroSchemaConverter().convert(message(type)));

// Wrong migration — old description used as message
assertThatThrownBy(() -> new AvroSchemaConverter().convert(message(type)))
    .isInstanceOf(IllegalArgumentException.class)
    .hasMessage("Should not allow TIME_MICROS with " + primitive);

// Correct migration — message from the throw site
assertThatThrownBy(() -> new AvroSchemaConverter().convert(message(type)))
    .isInstanceOf(IllegalArgumentException.class)
    .hasMessageContaining("…"); // actual text from throw new IllegalArgumentException(…)
```

## Bulk first pass

Start with a **syntax-only** bulk rewrite (search-and-replace or a scripted pass) that converts a subset of JUnit `Assert` calls and swaps imports. This is a mechanical first pass only — it does **not** produce finished AssertJ style on its own.

### What a bulk rewrite can safely do

| Input | Output |
|-------|--------|
| `assertEquals(expected, actual)` | `assertThat(actual).isEqualTo(expected)` |
| `assertArrayEquals(expected, actual)` | `assertThat(actual).isEqualTo(expected)` |
| `assertNull` / `assertNotNull` | `isNull` / `isNotNull` |
| `assertTrue` / `assertFalse` | `isTrue` / `isFalse` on the **same boolean expression** |
| `assertSame` / `assertNotSame` | `isSameAs` / `isNotSameAs` |
| `Assert.fail(…)` | AssertJ `fail(…)` |
| JUnit `Assert` / Hamcrest imports | AssertJ static imports (when Assert was present) |

### What the bulk pass does **not** handle (requires a manual second pass)

Everything in [`writing-tests.md`](./writing-tests.md) that is **not** in the table above — including but not limited to:

| Category | Left behind by script | Target |
|----------|----------------------|--------|
| **Exceptions** | `catchThrowable` + separate `assertThat(caught)` | `assertThatThrownBy` + `isInstanceOf` + `hasMessage*` |
| **Exceptions** | `@Test(expected)`, JUnit `assertThrows`, `TestUtils.assertThrows`, try/catch/`fail` | `assertThatThrownBy` + `isInstanceOf` + `hasMessage*` |
| **Exceptions** | `assertThatThrownBy` with only `isInstanceOf` | Add `hasMessage` / `hasMessageContaining` from the `throw` site |
| **Exceptions** | Old `assertThrows(description, …)` description copied into `hasMessage` | Real exception text, not the test description |
| **Collections** | `isEqualTo(Collections.emptyList())`, `isEqualTo(List.of(…))`, `isEqualTo(otherList)` | `isEmpty()`, `containsExactly(…)`, `containsExactlyElementsOf(…)` |
| **Collections** | `assertThat(x.contains(…)).isTrue()` / `isEmpty().isTrue()` / `size().isEqualTo(n)` | `contains`, `isEmpty`, `hasSize`, `hasSameSizeAs`, … |
| **Optional** | `assertThat(opt.isPresent()).isTrue()` / `isFalse()` | `assertThat(opt).isPresent()` / `isEmpty()` |
| **Numeric** | `assertThat(comparison).isTrue()` / `isFalse()` | `isZero`, `isPositive`, `isGreaterThan`, `hasSizeLessThan`, … |
| **Numeric** | `assertThat(x).isEqualTo(0)` on integral results | `assertThat(x).isZero()` |
| **Files** | `assertThat(file.exists()).isTrue()` | `assertThat(file).exists()` |
| **Strings** | `assertThat(x.toString())` | `assertThat(x).asString()` |
| **Assumptions** | `Assume.assumeTrue` / `Assumptions.assumeTrue` / `org.junit.jupiter.api.Assumptions` | AssertJ `assumeThat` |
| **Temp dirs** | `@Rule TemporaryFolder`, package-private `@TempDir` | `@TempDir private Path tempDir` (+ accessor for subclasses) |
| **Temp paths** | `toFile().getAbsolutePath()`, `new File(tempDir.toFile(), …)`, create+delete before writer | `tempDir.resolve(…)` + `new Path(….toUri())` |
| **Ordering** | `assertThat(a.compareTo(b) < 0).isTrue()` | `assertThat(a).isLessThan(b)` / `isEqualByComparingTo` |
| **Ordering** | `assertThat(compare(a, b)).isPositive()` etc. | Keep, but migrate from `assertTrue(compare(…) > 0)` |
| **Type / reference** | `instanceof` / `==` / `\|\|` wrapped in `isTrue()` | `isInstanceOf`, `isSameAs`, `isIn` |
| **Iterators** | Custom drain helpers (`assertIteratorEquals`, …) | `assertThat(it).toIterable().containsExactly(…)` |
| **Iterators** | `assertThat(it.hasNext()).isFalse()` / `assertFalse(it.hasNext())` | `assertThat(it).isExhausted()` |
| **Streams** | `assertThat(result).isEqualTo(stream.collect(…))` | `containsExactlyElementsOf(…)` after collecting |
| **Maps / sets** | `hasSize` + `containsKey` / `contains` on same subject | `containsOnlyKeys` / `containsExactlyInAnyOrder` |
| **No exception** | `assertTrue(true)`, empty try/catch | `assertThatCode(…).doesNotThrowAnyException()` |
| **Style** | `.as("…")` on `assertThatThrownBy` | Omit |
| **Style** | Block lambda with one statement in `assertThatThrownBy` | Expression lambda or method reference |
| **Bugs** | `assertThat(expr.method()).isEqualTo(…)` — AssertJ chained on the **value** | `assertThat(expr).method(…)` — chain on the **assertion** |

**A bulk rewrite is not sufficient.** After any bulk `assertEquals` → `assertThat` edit, always run the post-migration audit below before considering a module done.

## Post-migration audit

Run these checks on the migrated tree. Review every hit — some patterns are heuristics and may have false positives (e.g. `isEqualTo(0)` on a non-numeric field).

Replace `<module-test>` with the module's `src/test` directory (e.g. `parquet-hadoop/src/test`).

### JUnit / Hamcrest leftovers

```bash
rg 'import static org\.junit\.Assert|import org\.junit\.Assert|org\.hamcrest|org\.junit\.Assume|Assumptions\.assumeTrue' <module-test>
```

### Exceptions

```bash
# Not yet migrated to AssertJ
rg '@Test\(expected|TestUtils\.assertThrows|\bassertThrows\(' <module-test>
rg 'catchThrowable\(' <module-test>

# Type-only exception checks (missing message assertion)
rg 'assertThatThrownBy\([^;]+\)\s*\.isInstanceOf\([^)]+\)\s*;' <module-test>

# Invalid or discouraged
rg 'assertThatThrownBy\([^)]+\)\s*\.as\(' <module-test>
rg 'hasMessage\(\(String\) null\)|hasMessage\(null\)' <module-test>

# try/catch used only to assert an exception (manual review)
rg -U 'catch\s*\([^)]+\)\s*\{[^}]*fail\(' <module-test>
```

When converting `TestUtils.assertThrows` or JUnit `assertThrows`, add `hasMessage` from the production `throw` statement — **not** from the old test description string.

### Collections and sizes

```bash
rg 'isEqualTo\(Collections\.emptyList\(\)\)|isEqualTo\(List\.of\(|isEqualTo\(java\.util\.List\.of\(' <module-test>
rg '\.contains\([^)]+\)\.(isTrue|isFalse)\(\)' <module-test>
rg '\.isEmpty\(\)\.(isTrue|isFalse)\(\)' <module-test>
rg 'assertThat\([^)]+\.size\(\)\)\.' <module-test>
rg '\.hasSize\([^)]+\.(length|size)\(\)\)' <module-test>
rg 'assertIteratorEquals|assertAllRowsEqual' <module-test>
rg 'assertThat\([^)]+\.isPresent\(\)\)\.(isTrue|isFalse)' <module-test>
```

### Iterators and streams

```bash
rg 'assertThat\([^)]+\.hasNext\(\)\)\.(isTrue|isFalse)' <module-test>
rg 'assertFalse\([^)]+\.hasNext\(\)\)' <module-test>
rg 'assertThat\([^)]+\)\.isEqualTo\([^)]*\.stream\(\)' <module-test>
rg 'assertThat\([^)]+\.length\)\.isEqualTo' <module-test>
```

### Numeric and boolean comparisons

```bash
rg 'assertThat\([^)]*[<>!=]=?[^)]*\)\.(isTrue|isFalse)\(\)' <module-test>
rg 'assertThat\([^)]+\)\.isEqualTo\(0\)|assertThat\([^)]+\)\.isEqualTo\(0L\)' <module-test>
```

Manually fix hits such as `assertThat(file.length()).isEqualTo(0)` → `isZero()`, `assertThat(x).isPositive()`, etc.

### Files, strings, assumptions

```bash
rg '\.exists\(\)\)\.(isTrue|isFalse)|assertTrue\([^)]*\.exists\(\)' <module-test>
rg 'assertThat\([^)]+\.toString\(\)\)' <module-test>
rg 'Assume\.assumeTrue|Assumptions\.assumeTrue|org\.junit\.jupiter\.api\.Assumptions' <module-test>
rg -U '@TempDir\s*\n\s*(public |protected )?[A-Za-z]' <module-test>
rg 'TemporaryFolder|@Rule.*[Tt]emp' <module-test>
rg 'tempDir\.toFile\(\)|new File\(tempDir\.toFile\(\)|toFile\(\)\.getAbsolutePath\(\)' <module-test>
rg 'Files\.createFile\(tempDir' <module-test>
rg 'AvroDirectWriterTest' <module-test>
```

### Ordering, type, and reference equality

```bash
rg 'compareTo\([^)]+\)\s*(<|>|==|!=)' <module-test>
rg 'assertThat\([^)]*instanceof[^)]*\)\.(isTrue|isFalse)' <module-test>
rg 'assertThat\([^)]*==[^)]*\)\.isTrue' <module-test>
rg 'assertThat\([^)]*\|\|[^)]*\)\.isTrue' <module-test>
```

### Broken or flipped assertion chains

```bash
# AssertJ API wrongly nested inside assertThat(...) — often a compile error or silent wrong subject
rg 'assertThat\([^)]+\.(isEqualTo|isNull|isNotNull|isTrue|isFalse|contains|hasSize)\(' <module-test>

# Literal or constant as subject (heuristic — review each hit)
rg 'assertThat\((true|false|null|\d+|"[^"]*")\)' <module-test>
```

### No exception expected

```bash
rg 'assertTrue\(true\)|assertThat\(true\)\.isTrue' <module-test>
rg '\bassert\s+' <module-test>
```

## Suggested workflow

1. Do a bulk first pass on `<module-test>` (search-and-replace or scripted), or migrate by hand.
2. Fix compile errors (wrong generics on `assertThatThrownBy`, broken nesting, etc.).
3. Run **all** audit `rg` commands above; fix every real violation.
4. `./mvnw spotless:apply -pl <module>`
5. `mvn -pl <module> test` (or `test-compile` if full test run is blocked).

A module is not migration-complete until the audit is clean (aside from documented exceptions) **and** tests pass.

## Partial migration

When only exception tests are migrated in a file that otherwise stays on JUnit 4:

- Add AssertJ imports only for `assertThatThrownBy` / `assertThatCode`
- Leave existing `assertEquals` / `assertTrue` unchanged unless explicitly migrating the whole file

When the **whole file or module** is being migrated, ignore the “leave unchanged” rule — apply all conventions in [`writing-tests.md`](./writing-tests.md) and run the post-migration audit.

## Verification

After changes, apply formatting, run the post-migration audit, and run tests for the affected module:

```bash
./mvnw spotless:apply -pl <module>
mvn -pl <module> test
```

Example:

```bash
# bulk first pass on parquet-hadoop/src/test (search-and-replace or by hand)
# … run audit rg commands from Post-migration audit …
./mvnw spotless:apply -pl parquet-hadoop
mvn -pl parquet-hadoop test
```

Run `spotless:apply` before committing so import order and formatting match the project style.
