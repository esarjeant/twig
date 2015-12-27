# Cassandra Twig JDBC Driver
This is a fork of the Cassandra JDBC Driver:

[Cassandra JDBC Driver](https://code.google.com/a/apache-extras.org/p/cassandra-jdbc)

The purpose of this modification is to provide a JDBC compliant driver for
Cassandra that has functional query support for IntelliJ. While the original driver
is quite capable, it lacks a few key things:

- Thrift 2.1
- Table name support for MetaData
- ResultSet support for LIST, SET or MAP via JDBC getObject()
- JUnit tests for these things
- Whatever else comes along that is necessary for the IntelliJ query tool.

This is hosted on Github as a result of the demise of Google Code where it was hosted
originally.
