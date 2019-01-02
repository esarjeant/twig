[![Build Status](https://travis-ci.org/esarjeant/twig.svg?branch=develop)](https://travis-ci.org/esarjeant/twig)
# Cassandra Twig JDBC Driver
This is a fork of the Cassandra JDBC Driver:

[Cassandra JDBC Driver](https://code.google.com/a/apache-extras.org/p/cassandra-jdbc)

The purpose of this modification is to provide a JDBC compliant driver for
Cassandra that has functional query support for IntelliJ. While the original driver
is quite capable, it lacks a few key things:

- Cassandra 2.x/3.x Driver Support
- Table name support for MetaData
- ResultSet support for LIST, SET or MAP via JDBC getObject()
- JUnit tests for these things
- Whatever else comes along that is necessary for the IntelliJ query tool.

This is hosted on Github as a result of the demise of Google Code where it was hosted originally.

Versions now follow the release cycle of the Cassandra drives. So Twig 2.1 will be 
compiled with the 2.1.x Cassandra native drivers. If you are looking at Cassandra 3.0, for example,
you would ideally want Twig 3.0. 

Minor revisions of the Twig driver do not follow incremental versions from the Cassandra driver.
So 2.1.1 would in fact be the first point release for Twig in the 2.1 series but may not yet be using
the 2.1.1 DataStax driver.

Release Notes - 3.0.1
---------------------
Support for materialized views; meta-data enhancements to query via Cassandra 3.x Cluster rather than using
raw CQL statements against the system_schema.

Add support for additional native data-types in Cassandra 3.x:

* LocalDate
* LocalTime

Update conversion logic for LIST, SET and MAP types returning from Cassandra. Clarify unit tests surrounding
these data-types. 

Release Notes - 3.0.0
---------------------
First version to provide Cassandra 3.x support. This release also requires JDK 1.7 or later to run.

Release Notes - 2.1.1
---------------------
Adding support for quirks mode in IntelliJ and DbVisualizer. Moved all parameters to the JDBC _Parameters_ section
and away from the URL; this includes:

* `ssltruststore`: Full path to a Java trust store with the certificate required for SSL communications.
* `ssltrustpass`: Password for the specified truststore.
* `ssltrusttype`: Encoding type for the trust store, this is usually JKS.
* `sslenable`: Toggle to `true` to enable SSL; requires `truststore` and `trustpass` to be set correctly.
* `intellijQuirks`: Set `true` to enable quirks processing for IntelliJ.
* `dbvisQuirks`: Set `true` to enable quirks processing for DbVisualizer.
* `logpath`: Location for CQL query log; default is not set.
* `logenable`: Toggle to `true` to enable CQL query logging.

Quirks processing is particularly helpful for IntelliJ; it makes it possible to browse tabular data without errors.

Additionally, converts the build strategy to Gradle. The embedded Cassandra server provided by cassandra-unit is 
integrated with the build/test strategy and tests now use a base class for core connectivity needs.

The URL format specified from the previous release has been deprecated by Parameters. Use your JDBC client to
more easily configure these settings.

Release Notes - 2.1.0
---------------------
The 2.1 version of Twig should be considered a _major_ rewrite. As of this version,
the Thrift drivers have been deprecated and replaced with native Cassandra
driver adapters.

Support for encrypted connections is now included. This can be invoked using URL parameters 
in the driver query string:

    jdbc:cassandra://localhost:9042/mykeyspace?trustStore=%2Ftmp%2Fcassandra_truststore&trustPass=cassandra
    
The trustStore URL must be properly escaped for it to work. Also be aware that the default 
port number has moved away from Thrift (9160) to the native Cassandra client port 9042. If
you are still using Thrift then Twig 2.x will not be suitable for your needs.