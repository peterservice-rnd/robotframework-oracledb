RobotFramework Oracle Library
=================================

|Build Status|

Short Description
-----------------

`Robot Framework`_ library for working with Oracle database, using `cx_Oracle`_.

Installation
------------

::

    pip install robotframework-oracledb

Documentation
-------------

See keyword documentation for robotframework-oracledb library in
folder ``docs``.

Example
-------
+-----------+------------------+
| Settings  |      Value       |
+===========+==================+
|  Library  |     OracleDB     |
+-----------+------------------+

+---------------+---------------------------------------+--------------------+--------------------------+----------+
|  Test cases   |                  Action               |      Argument      |         Argument         | Argument |
+===============+=======================================+====================+==========================+==========+
|  Simple Test  | OracleDB.Connect To Oracle            | rb60db             | username                 | password |
+---------------+---------------------------------------+--------------------+--------------------------+----------+
|               | @{query}=                             | Execute Sql String | select sysdate from dual |          |
+---------------+---------------------------------------+--------------------+--------------------------+----------+
|               | OracleDB.Close All Oracle Connections |                    |                          |          |
+---------------+---------------------------------------+--------------------+--------------------------+----------+

License
-------

Apache License 2.0

.. _Robot Framework: http://www.robotframework.org
.. _cx_Oracle: http://cx-oracle.readthedocs.io

.. |Build Status| image:: https://travis-ci.org/peterservice-rnd/robotframework-oracledb.svg?branch=master
   :target: https://travis-ci.org/peterservice-rnd/robotframework-oracledb