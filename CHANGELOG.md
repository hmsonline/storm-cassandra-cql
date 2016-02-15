## 0.3.4

* [#60][]: Bump cassandra-driver-core dependency to 2.1.9

[#60]: https://github.com/hmsonline/storm-cassandra-cql/issues/60

## 0.3.3

* [#55][]: Add support for QueryLogger into MapConfiguredCqlClientFactory

[#55]: https://github.com/hmsonline/storm-cassandra-cql/issues/55

## 0.3.2

* [#54][]: Reduce synchronization on CqlClientFactory

[#54]: https://github.com/hmsonline/storm-cassandra-cql/issues/54

## 0.3.1

* [#51][]: Bump cassandra-driver-core dependency to 2.1.6

[#51]: https://github.com/hmsonline/storm-cassandra-cql/issues/51

## 0.3.0

* [#31][]: Added ability to configure read timeouts, and various other parameters on the CQL cluster/session.
* [#47][]: Added LZ4 dependency, so it gets bundled in.
* [#37][]: Fixed leaking statements inside State object.
* [#36][]: Fixed issue with incrmental state only commiting one aggregate value.
* Added cassandra-unit to the test suite so we could un-Ignore tests.
* Repackaged tests and added documentation to make them more understantable.

[#31]: https://github.com/hmsonline/storm-cassandra-cql/issues/31
[#36]: https://github.com/hmsonline/storm-cassandra-cql/issues/36
[#37]: https://github.com/hmsonline/storm-cassandra-cql/issues/37
[#47]: https://github.com/hmsonline/storm-cassandra-cql/issues/47

## 0.2.4

* Bump cassandra-driver-core dependency to 2.1.4

## 0.2.1

* [#27][]: Use QUORUM by default instead of LOCAL_QUORUM

## 0.2.0

* [#22][] / [#26][]: Explicit consistency levels for batches, cluster, and conditional updates

[#22]: https://github.com/hmsonline/storm-cassandra-cql/issues/22
[#26]: https://github.com/hmsonline/storm-cassandra-cql/issues/26
[#27]: https://github.com/hmsonline/storm-cassandra-cql/issues/27

