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

# Parquet Developer Scripts
This directory contains scripts useful to developers when packaging,
testing, or committing to Parquet.

Merging a pull request requires being a committer on the project.

* How to merge a Pull request:
have an apache and apache-github remote setup
```
git remote add apache-github https://github.com/apache/parquet-java.git
git remote add apache https://gitbox.apache.org/repos/asf?p=parquet-java.git
```
run the following command
```
dev/merge_parquet_pr.py
```

Note:
* The parent directory of your parquet repository must be called parquet-java
* Without jira-python installed you'll have to close the JIRA manually

example output:
```
Which pull request would you like to merge? (e.g. 34):
```
Type the pull request number (from https://github.com/apache/parquet-java/pulls) and hit enter.
```
=== Pull Request #X ===
title	Blah Blah Blah
source	repo/branch
target	master
url	https://api.github.com/repos/apache/parquet-java/pulls/X

Proceed with merging pull request #3? (y/n):
```
If this looks good, type y and hit enter.
```
From git-wip-us.apache.org:/repos/asf/parquet-java.git
 * [new branch]      master     -> PR_TOOL_MERGE_PR_3_MASTER
Switched to branch 'PR_TOOL_MERGE_PR_3_MASTER'

Merge complete (local ref PR_TOOL_MERGE_PR_3_MASTER). Push to apache? (y/n):
```
A local branch with the merge has been created.
type y and hit enter to push it to apache master
```
Counting objects: 67, done.
Delta compression using up to 4 threads.
Compressing objects: 100% (26/26), done.
Writing objects: 100% (36/36), 5.32 KiB, done.
Total 36 (delta 17), reused 0 (delta 0)
To git-wip-us.apache.org:/repos/asf/parquet-java.git
   b767ac4..485658a  PR_TOOL_MERGE_PR_X_MASTER -> master
Restoring head pointer to b767ac4e
Note: checking out 'b767ac4e'.

You are in 'detached HEAD' state. You can look around, make experimental
changes and commit them, and you can discard any commits you make in this
state without impacting any branches by performing another checkout.

If you want to create a new branch to retain commits you create, you may
do so (now or later) by using -b with the checkout command again. Example:

  git checkout -b new_branch_name

HEAD is now at b767ac4... Update README.md
Deleting local branch PR_TOOL_MERGE_PR_X
Deleting local branch PR_TOOL_MERGE_PR_X_MASTER
Pull request #X merged!
Merge hash: 485658a5

Would you like to pick 485658a5 into another branch? (y/n):
```
For now just say n as we have 1 branch

# Release Verification

The Apache Arrow Release Approval process follows the guidelines defined at the
`Apache Software Foundation Release Approval <https://www.apache.org/legal/release-policy.html#release-approval>`_.

For a release vote to pass, a minimum of three positive binding votes and more
positive binding votes than negative binding votes MUST be cast.
Releases may not be vetoed. Votes cast by PMC members are binding, however,
non-binding votes are greatly encouraged and a sign of a healthy project.

In order to cast a vote individuals are expected to follow the following steps.

## Download source package, signature file, hash file and KEYS

The Release candidate will be present at `https://dist.apache.org/repos/dist/dev/parquet/`.
The RC folder will depend on the version and the release candidate id. See the following example files for
Apache Parquet 1.15.0 RC 1:
```
wget https://dist.apache.org/repos/dist/dev/parquet/apache-parquet-1.15.0-rc1/apache-parquet-1.15.0.tar.gz
wget https://dist.apache.org/repos/dist/dev/parquet/apache-parquet-1.15.0-rc1/apache-parquet-1.15.0.tar.gz.asc
wget https://dist.apache.org/repos/dist/dev/parquet/apache-parquet-1.15.0-rc1/apache-parquet-1.15.0.tar.gz.sha512
wget https://dist.apache.org/repos/dist/release/parquet/KEYS
```

## Verify signature and hash

GnuPG is recommended, which can be install by:
- `yum install gnupg`, `apt-get install gnupg` on Linux based environments.
- `brew install gnupg` on macOS environments.


```
gpg --import KEYS
gpg --verify apache-parquet-1.15.0.tar.gz.asc apache-parquet-1.15.0.tar.gz
sha512sum --check apache-parquet-1.15.0.tar.gz.sha512
```

## Verify license header

Apache RAT is recommended to verify the license header, which can be dowload with the following command.

```
wget https://archive.apache.org/dist/creadur/apache-rat-0.16.1/apache-rat-0.16.1-bin.tar.gz
tar zxvf apache-rat-0.16.1-bin.tar.gz
```

You can check with the following command.
It will output a file list which doesn't include ASF license headers.
Please substitute `$PARQUET_SRC_FOLDER` with your `parquet-java` source folder from the following command.

```
java  -jar apache-rat-0.16.1/apache-rat-0.16.1.jar -a -d apache-parquet-1.15.0.tar.gz -E $PARQUET_SRC_FOLDER/.rat-excludes.txt
```

## Verify building and tests

Check the [building section](../README.md#building)
