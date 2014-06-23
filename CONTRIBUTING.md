# Contributing
We prefer to receive contributions in the form of GitHub pull requests. Please send pull requests against the [github.com/apache/incubator-parquet-mr](https://github.com/apache/incubator-parquet-mr) repository. If you've previously forked Parquet from its old location, you will need to add a remote or update your origin remote to https://github.com/apache/incubator-parquet-mr.git
Here are a few tips to get your contribution in:

  1. Break your work into small, single-purpose patches if possible. It’s much harder to merge in a large change with a lot of disjoint features.
  2. Create a JIRA for your patch on the [Parquet Project JIRA](https://issues.apache.org/jira/browse/PARQUET).
  3. Submit the patch as a GitHub pull request against the master branch. For a tutorial, see the GitHub guides on forking a repo and sending a pull request. Prefix your pull request name with the JIRA name (ex: https://github.com/apache/incubator-parquet-mr/pull/5).
  4. Make sure that your code passes the unit tests. You can run the tests with `mvn test` in the root directory. 
  5. Add new unit tests for your code. 

If you’d like to report a bug but don’t have time to fix it, you can still post it to our [issue tracker](https://issues.apache.org/jira/browse/PARQUET), or email the mailing list (dev@parquet.incubator.apache.org).
