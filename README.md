Apache Cassandra JOIN POC
-----------------

Note: This in no way represents the code that would be submitted PR to C*.
It takes a bunch of shortcuts to allow evalutation of:
1. is this possible?
2. what is performance like without optimisation?

You can load the northwind database by runnin ght following command:

`bin/cqlsh < northwind.cql`


You can do basic joins with or without filtering:

```
cqlsh:northwind> SELECT c.id AS "cid", c.first_name, o.id, e.id AS "eid", e.first_name AS "ename" FROM customers c INNER JOIN orders o ON c.id = o.customer_id INNER JOIN employees e ON e.id = o.employee_id WHERE c.id=41 ALLOW FILTERING;

 cid | first_name | id   | eid | ename
-----+------------+------+-----+---------
  41 |      Linda | 4188 | 216 |   Kathy
  41 |      Linda | 4066 | 205 |   Annie
  41 |      Linda | 4409 | 209 |    Tina
  41 |      Linda | 4496 | 218 | Frances
  41 |      Linda | 4065 | 205 |   Annie
  41 |      Linda | 4168 | 201 |  George
  41 |      Linda | 4314 | 207 |  Willie
```

In this case orders needs an index on customer_id. (Not strictly necessary if we do more complicated preprocessing)

```
cqlsh:northwind> SELECT c.id AS "cid", c.first_name, o.id, e.id AS "eid", e.first_name AS "ename" FROM customers c INNER JOIN orders o ON c.id = o.customer_id INNER JOIN employees e ON e.id = o.employee_id WHERE c.id=41;
InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot INNER JOIN orders AS o ON c.id = o.customer_id. Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"
```
Adding the index will remove the need for filtering:

```
cqlsh:northwind> CREATE INDEX orders_by_customer ON orders (customer_id);
cqlsh:northwind> SELECT c.id AS "cid", c.first_name, o.id, e.id AS "eid", e.first_name AS "ename" FROM customers c INNER JOIN orders o ON c.id = o.customer_id INNER JOIN employees e ON e.id = o.employee_id WHERE c.id=41;
 cid | first_name | id   | eid | ename
-----+------------+------+-----+---------
  41 |      Linda | 4188 | 216 |   Kathy
  41 |      Linda | 4066 | 205 |   Annie
  41 |      Linda | 4409 | 209 |    Tina
  41 |      Linda | 4496 | 218 | Frances
  41 |      Linda | 4065 | 205 |   Annie
  41 |      Linda | 4168 | 201 |  George
  41 |      Linda | 4314 | 207 |  Willie
```

LEFT and RIGHT JOINs work:

```
cqlsh:northwind> SELECT c.id AS "cid", c.first_name, o.id, e.id AS "eid", e.first_name AS "ename" FROM customers c LEFT JOIN orders o ON c.id = o.customer_id INNER JOIN employees e ON e.id = o.employee_id WHERE c.id=101;

 cid | first_name | id   | eid  | ename
-----+------------+------+------+-------
 101 |       Bryn | null | null |  null
```
```
cqlsh:northwind> SELECT c.id AS "cid", c.first_name, o.id, e.id AS "eid", e.first_name AS "ename" FROM orders o RIGHT JOIN customers c ON c.id = o.customer_id INNER JOIN employees e ON e.id = o.employee_id WHERE c.id=101;

 cid | first_name | id   | eid  | ename
-----+------------+------+------+-------
 101 |       Bryn | null | null |  null
```

Adding additional WHERE clauses works, but you have to allow filtering or create a materialized view if your condition is not part of the primary index.
```
cqlsh:northwind> SELECT c.id AS "cid", c.first_name, o.id, e.id AS "eid", e.first_name AS "ename" FROM customers c INNER JOIN orders o ON c.id = o.customer_id INNER JOIN employees e ON e.id = o.employee_id WHERE c.id=41 AND e.first_name = 'Annie' ALLOW FILTERING;

 cid | first_name | id   | eid | ename
-----+------------+------+-----+-------
  41 |      Linda | 4066 | 205 | Annie
  41 |      Linda | 4065 | 205 | Annie
```
