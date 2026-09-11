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

# Writing tests (JUnit 5 + AssertJ)

Conventions for **writing new tests** and **reviewing new/changed tests** in this
repository using [AssertJ](https://assertj.github.io/doc/). Use JUnit 5
(`org.junit.jupiter.api.*`) for test lifecycle and AssertJ for all assertions,
exceptions, and assumptions.

> Migrating an existing JUnit 4 test to this style? Start with
> [`migrating-tests.md`](./migrating-tests.md) — it covers the mechanical first
> pass, the JUnit → AssertJ mapping table, and the post-migration audit. The
> conventions below define the **target style** those migrations aim for.

## Imports

- Use `import static org.assertj.core.api.Assertions.assertThat;`
- Use `import static org.assertj.core.api.Assertions.assertThatThrownBy;` for exception tests
- Use `import static org.assertj.core.api.Assertions.assertThatCode;` when verifying no exception is thrown
- Use `import static org.assertj.core.api.Assumptions.assumeThat;` for conditional test execution (skip when assumption fails) — **not** JUnit `Assume`, JUnit 5 `Assumptions`, or any other assumption API
- **Do not** import from `AssertionsForClassTypes` unless there is a genuine generic ambiguity (e.g. `invoke()` return values). Prefer `Assertions` first.
- Use JUnit 5 (`org.junit.jupiter.api.*`) for test lifecycle only — `@Test`, `@BeforeEach`, `@AfterEach`, `@TempDir`, etc. Use AssertJ for assertions, exceptions, and assumptions.

## `@TempDir`

Use JUnit 5 `@TempDir` instead of JUnit 4 `TemporaryFolder` / `@Rule`:

```java
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;

@TempDir
private Path tempDir;
```

- Declare `@TempDir` fields **`private`** by default — not package-private or `public`.
- Prefer `java.nio.file.Path` on the test class; expose a `protected` accessor (e.g. `getTempFolder()`) when subclasses need the directory.
- When both `java.nio.file.Path` and `org.apache.hadoop.fs.Path` are in scope, declare `@TempDir` as `private java.nio.file.Path tempDir` and **do not** import `java.nio.file.Path` if it would clash with Hadoop `Path`.

```java
// Wrong — package-private or public
@TempDir
Path tempDir;

// Wrong — JUnit 4
@Rule
public TemporaryFolder temp = new TemporaryFolder();

// Correct
@TempDir
private Path tempDir;

// Correct when Hadoop Path is also used
@TempDir
private java.nio.file.Path tempDir;
```

### Path handling (`@TempDir` → Hadoop `Path` / `File`)

Stay on `java.nio.file.Path` for temp-dir work. Convert to Hadoop or `java.io.File` only at API boundaries.

**Hadoop `Path` from `@TempDir`:**

```java
// Correct — child path for a writer/reader
Path output = new Path(tempDir.resolve("out.parquet").toUri());

// Correct — unique file that must not exist yet (CREATE mode)
Path output = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());

// Correct — directory root
Path root = new Path(tempDir.toUri());
```

Prefer `.toUri()` when constructing Hadoop `Path` from `java.nio.file.Path` — do not round-trip through `toFile().getAbsolutePath()` or `toString()` on an intermediate variable unless the API requires a `String`.

**`java.io.File` when required:**

```java
File file = tempDir.resolve("child.parquet").toFile();
```

Use `tempDir.resolve("name")`, not `new File(tempDir.toFile(), "name")`.

**Do not pre-create files for Hadoop CREATE-mode writers.** Parquet/Hadoop writers open with create-if-not-exists semantics and fail with `FileAlreadyExistsException` when the path already exists. A UUID (or other unique name) under `@TempDir` is enough — no `Files.createFile` / `temp.newFile()` + delete dance.

```java
// Wrong — creates then deletes, or leaves a file that breaks CREATE
File temp = Files.createTempFile(tempDir, "test", ".tmp").toFile();
Path path = new Path(temp.getAbsolutePath());

java.nio.file.Path tempFile = Files.createFile(tempDir.resolve(UUID.randomUUID().toString()));
Files.delete(tempFile);
Path path = new Path(tempFile.toString());

// Wrong — redundant conversion
new Path(tempDir.toFile().getAbsolutePath())
new Path(new File(tempDir.toFile(), "out").getAbsolutePath())

// Correct — path does not exist yet; writer creates it
Path path = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
```

**`Files.createTempFile` / `Files.createTempDirectory`:** keep only when the test needs JDK temp-name generation inside `@TempDir` (e.g. a specific prefix/suffix). If the created path is passed to a CREATE-mode writer, delete it first with `Files.delete(path)`. Prefer `tempDir.resolve(...)` or `Files.createTempDirectory(tempDir, "prefix")` + `new Path(dir.toUri())` over `.toFile().getAbsolutePath()`.

**Shared test bases:** use `org.apache.parquet.DirectWriterTest` from the `parquet-hadoop` test-jar for direct `RecordConsumer` writing — do not duplicate it in other modules (e.g. no module-local `AvroDirectWriterTest`).

```java
import org.apache.parquet.DirectWriterTest;

public class TestArrayCompatibility extends DirectWriterTest {
```

## Actual vs expected order

AssertJ uses **actual first, expected second**:

```java
// Correct
assertThat(stream.read()).isEqualTo(124);
assertThat(column("a").compareTo(column("b"))).isEqualTo(-1);
assertThat(cw.getEncoding()).isEqualTo(PLAIN_DICTIONARY);

// Wrong (flipped)
assertThat(124).isEqualTo(stream.read());
assertThat(PLAIN_DICTIONARY).isEqualTo(cw.getEncoding());
```

## Assumptions

Use AssertJ `assumeThat` (`import static org.assertj.core.api.Assumptions.assumeThat`) to skip tests when a precondition is not met (OS-specific tests, optional features, etc.). Do **not** use JUnit 4 `Assume`, JUnit 5 `org.junit.jupiter.api.Assumptions`, or any JUnit assumption helper.

Prefer fluent assertions on the value under test over wrapping a boolean expression in `.isTrue()`:

```java
// Wrong — JUnit 4
Assume.assumeTrue(System.getProperty("os.name").toLowerCase().startsWith("win"));

// Wrong — JUnit 5
Assumptions.assumeTrue(System.getProperty("os.name").toLowerCase().startsWith("win"));

// Wrong — JUnit 5 static import of Assumptions (still JUnit, not AssertJ)
import static org.junit.jupiter.api.Assumptions.*;
assumeTrue(featureEnabled);

// Correct — AssertJ assumeThat
assumeThat(featureEnabled).isTrue();

// Correct — fluent check on the subject
assumeThat(System.getProperty("os.name").toLowerCase()).startsWith("win");
```

When an assumption fails, AssertJ throws `org.opentest4j.TestAbortedException` (same as JUnit 5), so the test is reported as skipped.

## Exception assertions

Use `assertThatThrownBy` for expected exceptions:

```java
assertThatThrownBy(() -> action())
    .isInstanceOf(SomeException.class)
    .hasMessage("exact message");
```

Every `assertThatThrownBy` chain **must** include `.isInstanceOf(...)` **and** a message assertion (`.hasMessage`, `.hasMessageContaining`, or `.hasMessageStartingWith`) unless the thrown exception reliably has a `null` message (see below).

**Always** use `assertThatThrownBy` — not `catchThrowable` followed by `assertThat(caught)`:

```java
// Wrong — split capture and assertion; does not fail clearly when no exception is thrown
Throwable caught = catchThrowable(() -> action());
assertThat(caught).isInstanceOf(BadConfigurationException.class);
assertThat(caught).hasMessageContaining("23");

// Correct
assertThatThrownBy(() -> action())
    .isInstanceOf(BadConfigurationException.class)
    .hasMessageContaining("23");
```

Use `.satisfies(…)`, `.hasSuppressedException(…)`, `.hasNoSuppressedExceptions()`, or `.isSameAs(…)` on the `assertThatThrownBy` chain when additional checks on the thrown instance are needed. Reserve `catchThrowable` only when the test must **continue after** capturing (e.g. multiple independent actions in one test) — not for a single expected failure.

`isInstanceOf` alone is **not** sufficient — it loses coverage of the error text users and operators actually see.

### Message checks

- **Always** add `.hasMessage(...)`, `.hasMessageContaining(...)`, or `.hasMessageStartingWith(...)` when the exception has a non-null message.
- Use `.hasMessage(...)` when the full message is stable and known (copy it from the `throw` site or a failing test).
- Use `.hasMessageContaining(...)` when the message includes variable detail (type names, field paths, etc.) or when several loop iterations share a common substring.
- **Omit** `hasMessage` only when the JDK exception message is reliably `null` (e.g. `EOFException`, `InvalidMarkException`, `ReadOnlyBufferException`, re-thrown checked exceptions with no message).
- **Never** use `.hasMessage(null)` or `.hasMessage((String) null)`.

```java
// Wrong — type only
assertThatThrownBy(() -> new AvroSchemaConverter().convert(parquetSchemaWithInt96))
    .isInstanceOf(IllegalArgumentException.class);

// Correct
assertThatThrownBy(() -> new AvroSchemaConverter().convert(parquetSchemaWithInt96))
    .isInstanceOf(IllegalArgumentException.class)
    .hasMessage(
        "INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.");

// Correct — shared substring across similar failures
assertThatThrownBy(() -> new AvroSchemaConverter().convert(message(type)))
    .isInstanceOf(IllegalArgumentException.class)
    .hasMessageContaining("Cannot annotate schema");
```

### `assertThatThrownBy` style

- **Do not** put `.as("description")` on `assertThatThrownBy` chains. Rely on the test method name or inline the intent in the lambda.
- **Do** keep `.as(DECIMAL)`, `.as(logicalType)`, etc. on **builder** chains inside the lambda — those are not AssertJ descriptions.
- Use an **expression lambda** for a single action. Reserve block lambdas (`() -> { … }`) for multiple statements.

```java
// Wrong — unnecessary block for one call
assertThatThrownBy(() -> {
      evaluate(neverCalled);
    })
    .isInstanceOf(ShortCircuitException.class)
    .hasMessage("…");

// Correct — expression lambda (or method reference when it fits)
assertThatThrownBy(() -> evaluate(neverCalled))
    .isInstanceOf(ShortCircuitException.class)
    .hasMessage("…");
assertThatThrownBy(command::run)
    .isInstanceOf(FileAlreadyExistsException.class)
    .hasMessageContaining("File already exists");

// Block lambda is fine when setup is required
assertThatThrownBy(() -> {
      MessageType incompatible = new MessageType("schema", …);
      readGroups(store, originalSchema, incompatible, 1);
    })
    .isInstanceOf(ParquetDecodingException.class)
    .hasMessage("…");
```

```java
// Wrong
assertThatThrownBy(() -> builder.build())
    .as("Should reject invalid type")
    .isInstanceOf(IllegalArgumentException.class);

// Correct
assertThatThrownBy(() -> builder.build())
    .isInstanceOf(IllegalArgumentException.class)
    .hasMessage("…");

// Builder .as() inside lambda is fine
assertThatThrownBy(() -> Types.required(INT32).as(DECIMAL).named("x"))
    .isInstanceOf(IllegalArgumentException.class)
    .hasMessage("…");
```

### No exception expected

Replace no-op `assertTrue(true)` after a successful call, try/catch/`Assert.fail` blocks, or Java `assert` statements with:

```java
assertThatCode(() -> uuidConverter.addBinary(binary)).doesNotThrowAnyException();
assertThatCode(() -> ToolRunner.run(conf, new Main(logger), args)).doesNotThrowAnyException();
```

## Numeric results

Prefer dedicated numeric assertions over boolean comparisons:

```java
// Wrong
assertThat(command.run()).isEqualTo(0);
assertThat(0 < file.length()).isTrue();
assertThat(columnSizeInBytes.get("DocId") > columnSizeInBytes.get("Num")).isTrue();

// Correct
assertThat(command.run()).isZero();
assertThat(file.length()).isPositive();
assertThat(columnSizeInBytes.get("DocId")).isGreaterThan(columnSizeInBytes.get("Num"));
```

Use `.isZero()`, `.isPositive()`, `.isNegative()`, `.isGreaterThan(...)`, etc. on the **value under test**.

## Files

```java
// Wrong
assertThat(output.exists()).isTrue();
assertThat(outputFile.length()).isEqualTo(0);

// Correct
assertThat(output).exists();
assertThat(outputFile.length()).isZero();
assertThat(avroFile.length()).isPositive();
```

## String / `toString()` comparisons

Use AssertJ's `.asString()` on the **subject**, not `.toString()` in `assertThat(...)`:

```java
// Wrong
assertThat(schema.toString()).isEqualTo(expectedMT.toString());
assertThat(stats.toString()).isEqualTo("min: 1.0, max: 2.0, num_nulls: 0");

// Correct — object compared to object (use .toString() on expected to avoid format overload)
assertThat(schema).asString().isEqualTo(expectedMT.toString());

// Correct — object compared to string literal
assertThat(stats).asString().isEqualTo("min: 1.0, max: 2.0, num_nulls: 0");
assertThat(pred).asString().isEqualTo("or(and(not(…");
assertThat(schemaString).contains("\"name\" : \"timestamp_1\"");
assertThat(schemaString).doesNotContain("\"type\" : [ \"null\", \"INT96\" ]");

// Correct — builder chain
assertThat(Types.required(FIXED_LEN_BYTE_ARRAY).length(16).as(uuidType()).named("uuid_field"))
    .asString()
    .isEqualTo("required fixed_len_byte_array(16) uuid_field (UUID)");
```

**Important:** `asString().isEqualTo(otherObject)` can hit AssertJ's `isEqualTo(String format, Object...)` overload. When comparing to another object, use `.isEqualTo(other.toString())` on the expected side.

`.as("description")` **before** `.asString()` is allowed on normal assertions:

```java
assertThat(records.get(0))
    .as("deserialization does not display the same result")
    .asString()
    .isEqualTo(r1.toString());
```

## Collections and sizes

Prefer AssertJ **collection assertions** on the collection itself. Assert on the collection (`list`, `conversions`, `records`, `encodings`, …), not on `size()`, `isEmpty()`, or `contains()` return values.

### Size

**Always** use `hasSize(n)` instead of `assertThat(collection.size()).isEqualTo(n)` or `assertThat(array.length).isEqualTo(n)`.

When comparing two collections (or a collection and an array) that should have the same length, prefer `hasSameSizeAs` over `hasSize(other.size())` or `hasSize(array.length)`:

```java
// Wrong
assertThat(splits.size()).isEqualTo(offsets.length);
assertThat(splits).hasSize(sizes.length);
assertThat(actual.size()).isEqualTo(expected.size());

// Correct
assertThat(splits).hasSameSizeAs(offsets);
assertThat(splits).hasSameSizeAs(sizes);
assertThat(actual).hasSameSizeAs(expected);
```

Use `hasSizeLessThan` / `hasSizeGreaterThan` when asserting a relative size bound, not a boolean comparison on sizes:

```java
// Wrong
assertThat(result.size() < DATA.size()).isTrue();

// Correct
assertThat(result).hasSizeLessThan(DATA.size());
```

### Membership, emptiness, and equality

Do not wrap collection state in a boolean and assert with `isTrue()` / `isFalse()`:

```java
// Wrong
assertThat(column.getEncodings().contains(Encoding.PLAIN_DICTIONARY))
    .as("Column should be dictionary encoded: " + name)
    .isTrue();
assertThat(column.getEncodings().contains(Encoding.PLAIN))
    .as("Column should not have plain data pages" + name)
    .isFalse();
assertThat(list).isEqualTo(List.of(1, 2, 3));
assertThat(list).isEqualTo(Collections.emptyList());
assertThat(list.size()).isEqualTo(3);
assertThat(list.isEmpty()).isTrue();
assertThat(!list.isEmpty()).isFalse();
assertThat(list.contains(item)).isTrue();
assertThat(metadata.getBlocks().isEmpty()).isFalse();
assertThat(metadata.getBlocks().size() > 0).isTrue();

// Correct
assertThat(column.getEncodings())
    .as("Column should be dictionary encoded: " + name)
    .contains(Encoding.PLAIN_DICTIONARY);
assertThat(column.getEncodings())
    .as("Column should not have plain data pages" + name)
    .doesNotContain(Encoding.PLAIN);
assertThat(list).containsExactly(1, 2, 3);
assertThat(list).isEmpty();
assertThat(list).isNotEmpty();
assertThat(list).hasSize(3);
assertThat(list).contains(item);
assertThat(metadata.getBlocks()).isNotEmpty();
```

Use `containsExactly` for ordered list equality with literal elements. Use `containsExactlyElementsOf` when comparing to another iterable. Use `containsExactlyInAnyOrder` only when order does not matter (e.g. `HashSet`-backed iterables).

**Exception:** `assertThat(stream.size()).isEqualTo(n)` on `BytesInput` / stream-like types is fine — `hasSize()` applies to collections, not arbitrary `size()` methods.

**Exception:** `Statistics.isEmpty()`, `hasNonNullValue()`, and similar domain `boolean` methods are not Java collections — keep `assertThat(stats.isEmpty()).isFalse()` or assert the specific stat fields directly.

## Optional (`java.util.Optional`)

Assert on the **Optional**, not on `optional.isPresent()` / `optional.isEmpty()` wrapped in `isTrue()` / `isFalse()`:

```java
// Wrong
assertThat(page.getCrc().isPresent()).as("Checksum was not set in page").isTrue();
assertThat(page.getCrc().isPresent()).as("Checksum was set in page").isFalse();
assertThat(offsetIndex.getUnencodedByteArrayDataBytes(0).isPresent()).isFalse();

// Correct
assertThat(page.getCrc()).as("Checksum was not set in page").isPresent();
assertThat(page.getCrc()).as("Checksum was set in page").isEmpty();
assertThat(offsetIndex.getUnencodedByteArrayDataBytes(0)).isEmpty();
```

For a present value, prefer `assertThat(opt).contains(value)` or `assertThat(opt).get().isEqualTo(value)` over `assertThat(opt.isPresent()).isTrue()` followed by `opt.get()`.

**Exception:** `Statistics.isEmpty()` and other domain `boolean` methods are not `Optional` — see Collections above.

## Streams and iterators

### Iterator content

Replace custom helpers that drain an iterator into a list/array and compare with `isEqualTo` (e.g. `assertIteratorEquals`, `assertAllRowsEqual`). Use AssertJ's `IteratorAssert.toIterable()` to consume the iterator and apply iterable assertions:

```java
// Wrong — custom helper draining the iterator
static void assertIteratorEquals(PrimitiveIterator.OfInt actualIt, int... expectedValues) {
  IntList actualList = new IntArrayList();
  actualIt.forEachRemaining(actualList::add);
  assertThat(actualList.toIntArray()).isEqualTo(expectedValues);
}
assertIteratorEquals(IndexIterator.all(10), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

// Correct — fluent AssertJ on the iterator
assertThat(IndexIterator.all(10)).toIterable().containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
assertThat(IndexIterator.intersection(lhs, rhs)).toIterable().isEmpty();
assertThat(ranges.iterator()).toIterable().containsExactly(1L, 2L, 3L, 4L);
```

Notes:

- `PrimitiveIterator.OfInt` / `OfLong` extend `Iterator`, so `assertThat(iterator)` works directly.
- `.toIterable()` **consumes** the iterator (same as a drain helper). Non-consuming checks use `hasNext()` / `isExhausted()` on the iterator itself (see below).
- For `PrimitiveIterator.OfLong`, `containsExactly` expects `Long` varargs — use `L` suffixes on literals (`1L`, `2L`, …).
- For `int...` expected values passed as a varargs parameter, box before spreading into `containsExactly`:

```java
assertThat(predicate.accept(ci))
    .toIterable()
    .containsExactly(Arrays.stream(expectedIndexes).boxed().toArray(Integer[]::new));
```

Do **not** add a custom AssertJ assertion class unless the same pattern is needed across many modules — `toIterable()` is sufficient.

### Iterator state (`hasNext` / `isExhausted`)

Assert on the **iterator**, not on `iterator.hasNext()` wrapped in `isTrue()` / `isFalse()`:

```java
// Wrong
assertThat(expIt.hasNext()).isFalse();
assertThat(expIt.hasNext()).isTrue();

// Correct
assertThat(expIt).isExhausted();
assertThat(expIt).hasNext();
```

Use `.isExhausted()` when all elements were consumed (e.g. after a partial match loop). Use `.hasNext()` when more elements are expected. `.as("…")` before these is allowed when the failure message needs context.

### `java.util.stream.Stream`

AssertJ has no `StreamAssert` — **collect first**, then assert on the resulting collection:

```java
// Wrong — boolean wrapper on stream terminal operation
assertThat(result.size() == expected.size()).isTrue();

// Wrong — isEqualTo on two independently collected lists (often fine, but prefer iterable API)
assertThat(result).isEqualTo(DATA.stream().filter(pred).collect(Collectors.toList()));

// Correct — collect actual, then compare as iterable
List<User> result = readFilteredUsers(filter);
assertThat(result).containsExactlyElementsOf(DATA.stream().filter(expectedFilter).collect(Collectors.toList()));

// Correct — known expected elements
assertThat(result).containsExactly(userA, userB);
```

For a `Stream` used only as an expected source in a helper, passing `stream.iterator()` to `assertThat(it).toIterable()` is fine when the stream is consumed once.

When asserting a **Java array** length, prefer `assertThat(array).hasSize(n)` over `assertThat(array.length).isEqualTo(n)`.

### Maps and sets — combine related checks

Prefer one assertion when size and membership are asserted together:

```java
// Wrong — three assertions for the same map
assertThat(map).hasSize(2);
assertThat(map).containsKey(keyA);
assertThat(map).containsKey(keyB);

// Correct
assertThat(map).containsOnlyKeys(keyA, keyB);

// Wrong — size + contains on a set
assertThat(set).hasSize(2);
assertThat(set).contains("hello", "world");

// Correct
assertThat(set).containsExactlyInAnyOrder("hello", "world");
```

Use `containsOnlyKeys` when the map must have **exactly** those keys. Use `containsExactlyInAnyOrder` for sets (order irrelevant). Use `containsExactly` when order matters.

### When `isTrue()` / `isFalse()` is still fine

Use `isTrue()` / `isFalse()` for genuine boolean expressions that are not collection, string, **type**, **reference equality**, or **membership** checks, e.g. `filter.keep(path)` or method calls that return `boolean` without a dedicated AssertJ assertion.

Do **not** use `assertThat(x instanceof Foo.class).isTrue()` — use `assertThat(x).isInstanceOf(Foo.class)` instead. For negation, use `isNotInstanceOf(...)`.

Do **not** use `assertThat(a == b).isTrue()` for reference equality — use `assertThat(a).isSameAs(b)` instead.

Do **not** use `assertThat(x == a || x == b).isTrue()` for “one of these values” — use `assertThat(x).isIn(a, b)` instead (or `isNotIn(...)` for negation).

```java
// Wrong
assertThat(records.get(0).get("dec") instanceof BigDecimal).isTrue();
assertThat(obj instanceof Map).as("Should be a map").isTrue();
assertThat(car.getDoors() == 4 || car.getDoors() == 5).isTrue();

// Correct
assertThat(records.get(0).get("dec")).isInstanceOf(BigDecimal.class);
assertThat(obj).as("Should be a map").isInstanceOf(Map.class);
assertThat(car.getDoors()).isIn(4, 5);
```

## Ordering and comparison

### `Comparable` / `compareTo`

```java
// Wrong
assertThat(a.compareTo(b) < 0).isTrue();
assertThat(a.compareTo(b) == 0).isTrue();

// Correct
assertThat(a).isLessThan(b);
assertThat(a).isGreaterThan(b);
assertThat(a).isEqualByComparingTo(b);
```

### `compare()` returning `int`

When the API returns `int` (not `Comparable` on the subject), assert on the result:

```java
assertThat(Float16.compare(a, b)).isZero();
assertThat(Float16.compare(a, b)).isPositive();
assertThat(Float16.compare(a, b)).isNegative();
```

For custom `Comparator` instances comparing values directly:

```java
assertThat(truncated)
    .usingComparator(comparator)
    .isLessThanOrEqualTo(value);
```

Do **not** use `usingComparator` with `Float16::compare` on boxed `Short` values — it does not match `Float16.compare(short, short)` semantics.

### Reference equality (`isSameAs`)

The **value under test** is the AssertJ subject; the known reference is the argument:

```java
// Correct — slice buffer is under test
assertThat(one.array()).as("Should use the same backing array").isSameAs(data.array());

// Correct — dictionary-encoded field should reuse the same object reference
assertThat(car.getModel()).isSameAs(previousCar.getModel());

// Wrong — backing array as subject
assertThat(data.array()).isSameAs(one.array());

// Wrong — reference equality via isTrue()
assertThat(car.model == previousCar.model).isTrue();
```

`.as("…")` on non-exception assertions is allowed.

## Arrays and bytes

```java
// Prefer
assertThat(bytes).containsExactly(1, 2, 3);
assertThat(bytes).hasSize(12);
assertThat(buffer).isEqualTo(expectedArray);

// Byte values from streams — cast to avoid JUnit-style promotion issues
assertThat((int) stream.read()).isEqualTo(i);
assertThat((byte) buffer.get()).isEqualTo((byte) i);
```

## Booleans and filters

Replace custom match helpers with direct AssertJ:

```java
// Wrong
assertMatches(filter, path);
assertDoesNotMatch(filter, path);

// Correct
assertThat(filter.keep(path)).isTrue();
assertThat(filter.keep(path)).isFalse();
```

## Patterns to avoid (quick reference)

| Avoid | Use instead |
|-------|-------------|
| `catchThrowable` + `assertThat(caught).isInstanceOf` / `hasMessage*` | `assertThatThrownBy(…).isInstanceOf(…).hasMessage*` |
| `assertThatThrownBy` without `hasMessage` | Add `hasMessage` / `hasMessageContaining` from the `throw` site |
| `@Rule TemporaryFolder` / `temp.newFile()` | `@TempDir private Path tempDir` + `tempDir.resolve(...)` |
| `tempDir.toFile().getAbsolutePath()` / `new File(tempDir.toFile(), …)` | `tempDir.resolve(…).toUri()` or `.toFile()` |
| `new Path(tempDir.toFile().getAbsolutePath())` | `new Path(tempDir.resolve(…).toUri())` |
| `Files.createFile` + `Files.delete` before CREATE-mode writer | `tempDir.resolve(uniqueName)` only — file must not exist |
| Duplicate `DirectWriterTest` in other modules | `extends DirectWriterTest` from `parquet-hadoop` test-jar |
| `Assume.assumeTrue(...)` / `Assumptions.assumeTrue(...)` | AssertJ `assumeThat(...).isTrue()` or fluent `assumeThat(value).…` |
| `singleElement()` | `containsExactly(element)` or explicit checks |
| `.satisfies(…)` | Direct assertions unless truly necessary |
| `assertThat(x.toString())` | `assertThat(x).asString()` |
| `assertThat(list.isEmpty()).isTrue()` | `assertThat(list).isEmpty()` |
| `assertThat(list.isEmpty()).isFalse()` / `assertThat(!list.isEmpty()).isTrue()` | `assertThat(list).isNotEmpty()` |
| `assertThat(opt.isPresent()).isTrue()` | `assertThat(opt).isPresent()` |
| `assertThat(opt.isPresent()).isFalse()` / `assertThat(opt.isEmpty()).isTrue()` | `assertThat(opt).isEmpty()` |
| `assertThat(list.contains(x)).isTrue()` | `assertThat(list).contains(x)` |
| `assertThat(list.contains(x)).isFalse()` | `assertThat(list).doesNotContain(x)` |
| `assertThat(list).isEqualTo(otherList)` | `assertThat(list).containsExactlyElementsOf(otherList)` |
| `assertThat(list.size()).isEqualTo(n)` | `assertThat(list).hasSize(n)` |
| `assertThat(list).hasSize(other.size())` / `hasSize(array.length)` | `assertThat(list).hasSameSizeAs(other)` |
| `assertThat(a.size() < b.size()).isTrue()` | `assertThat(a).hasSizeLessThan(b)` |
| `assertThat(array.length).isEqualTo(n)` | `assertThat(array).hasSize(n)` |
| `assertThat(it).toIterable()` for drained iterators | `assertThat(it).toIterable().containsExactly(1, 2, 3)` |
| `assertThat(it.hasNext()).isFalse()` | `assertThat(it).isExhausted()` |
| `assertThat(it.hasNext()).isTrue()` | `assertThat(it).hasNext()` |
| `assertThat(list).isEqualTo(stream.collect(toList()))` | `assertThat(list).containsExactlyElementsOf(stream.collect(toList()))` |
| `hasSize(n)` + `containsKey` / `contains` on same map or set | `containsOnlyKeys(…)` / `containsExactlyInAnyOrder(…)` |
| `assertThat(x instanceof Foo).isTrue()` | `assertThat(x).isInstanceOf(Foo.class)` |
| `assertThat(a == b).isTrue()` | `assertThat(a).isSameAs(b)` |
| `assertThat(x == a \|\| x == b).isTrue()` | `assertThat(x).isIn(a, b)` |
| `assertThat(x).isEqualTo(0)` on `int`/`long` results | `assertThat(x).isZero()` |
| `assertThat(0 < x).isTrue()` | `assertThat(x).isPositive()` |
| `assertThat(file.exists()).isTrue()` | `assertThat(file).exists()` |
| `assertThat(true).isTrue()` / try-catch for no exception | `assertThatCode(() -> …).doesNotThrowAnyException()` |
| `.as()` on `assertThatThrownBy` | (omit) |
| `assertThatThrownBy(() -> { action(); })` (single statement) | `assertThatThrownBy(() -> action())` or method reference |
| `hasMessage(null)` | (omit `hasMessage`) |
| Constant as assertion subject | Expression under test as subject |
| `hasCauseInstanceOf` when exception is thrown directly | Use `isInstanceOf` on the thrown type |

## Verification

After writing or changing tests, apply formatting and run tests for the affected module:

```bash
./mvnw spotless:apply -pl <module>
mvn -pl <module> test
```
