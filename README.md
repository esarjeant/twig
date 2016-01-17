[![Build Status](https://travis-ci.org/esarjeant/twig.svg?branch=develop)](https://travis-ci.org/esarjeant/twig)
# Cassandra Twig JDBC Driver
This is a fork of the Cassandra JDBC Driver:

[Cassandra JDBC Driver](https://code.google.com/a/apache-extras.org/p/cassandra-jdbc)

The purpose of this modification is to provide a JDBC compliant driver for
Cassandra that has functional query support for IntelliJ. While the original driver
is quite capable, it lacks a few key things:

- Cassandra 2.1 Driver Support
- Table name support for MetaData
- ResultSet support for LIST, SET or MAP via JDBC getObject()
- JUnit tests for these things
- Whatever else comes along that is necessary for the IntelliJ query tool.

This is hosted on Github as a result of the demise of Google Code where it was hosted
originally.

Versions now follow the release cycle of the Cassandra drives. So Twig 2.1 will be 
compiled with the 2.1.x Cassandra native drivers. If you are looking at Cassandra 3.0, for example,
you would ideally want Twig 3.0. 

Minor revisions of the Twig driver do not follow incremental versions from the Cassandra driver.
So 2.1.1 would in fact be the first point release for Twig in the 2.1 series but may not yet be using
the 2.1.1 DataStax driver.

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
