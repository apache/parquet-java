<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# parquet-format-structures

This module compiles the Parquet Thrift IDL (`parquet.thrift`) into Java classes
(`org.apache.parquet.format.*`) using the thrift compiler.

## Inlined IDL

`parquet.thrift` is kept as a verbatim in-tree copy at
`src/main/thrift/parquet.thrift`. The build reads it directly from there; no
download happens at build time.

The accompanying sidecar file `src/main/thrift/parquet-format.version` records
provenance:

```
parquet-format.commit=<full 40-char parquet-format commit sha>
parquet-format.version=<released version, e.g. 2.13.0, or UNRELEASED>
parquet-format.source=<GitHub blob URL at that commit>
```

- `parquet-format.commit` — the parquet-format commit the inlined IDL was taken
  from. Always a full 40-character sha.
- `parquet-format.version` — the released parquet-format version whose tag points
  at that commit, e.g. `2.13.0`. Set to `UNRELEASED` for POC/work-in-progress
  syncs that do not correspond to a released tag.
- `parquet-format.source` — convenience URL for browsing the upstream file at
  that exact commit.

## Upgrading the IDL

Use `dev/update-parquet-thrift.sh`. It resolves release tags with `git ls-remote`
and downloads the IDL from `raw.githubusercontent.com`, so it does not use the
GitHub REST API and is not subject to its rate limit. A failed update leaves the
inlined files untouched — the script writes to a temporary file, validates it,
and only moves it into place on success.

**POC / work-in-progress sync** (sets `version=UNRELEASED`):

```sh
dev/update-parquet-thrift.sh <full-40-char-commit-sha>
```

The commit sha must be exactly 40 hex characters — short shas cannot be fetched
from the remote.

**Release sync** (sets `version=<ver>`):

```sh
dev/update-parquet-thrift.sh --version <X.Y.Z>
```

This resolves the `apache-parquet-format-<X.Y.Z>` tag to its commit sha
automatically. You do not need to look up the sha yourself.

After running either form, rebuild and review:

```sh
./mvnw -pl parquet-format-structures -am install -DskipTests
git diff parquet-format-structures/src/main/thrift/parquet.thrift
```

## Release requirement

Before cutting a parquet-java release, the inlined IDL must match an official
parquet-format release. `dev/prepare-release.sh` enforces this automatically by
calling `dev/check-parquet-thrift-release.sh` before running `mvnw release:prepare`.

To prepare for a release, sync to the appropriate parquet-format version:

```sh
dev/update-parquet-thrift.sh --version <X.Y.Z>
```

The check fetches the canonical IDL via `git` and diffs it against the inlined
file. Any difference — including a `version=UNRELEASED` sidecar — fails the
check and aborts the release.
