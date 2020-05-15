/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.validation.operations;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.transport.messages.ResultMessage;


/**
 * Test column ranges and ordering with static column in table
 */
public class SelectJoinTest extends CQLTester
{

    private String customers;
    private String orders;
    private String employees;
    private String warehouses;

    @Before
    public void setup() throws Throwable
    {
        customers = keyspace() + "." + createTable("CREATE TABLE %s (id int, id2 int, id3 int, a int, b int, c int, primary key (id, id2, id3))");
        orders = keyspace() + "." + createTable("CREATE TABLE %s (id int, customer_id int, customer_id2 int, employee_id int, warehouse_id int, primary key (id))");
        employees = keyspace() + "." + createTable("CREATE TABLE %s (id int, primary key (id))");
        warehouses = keyspace() + "." + createTable("CREATE TABLE %s (id int, primary key (id))");
        executeFormattedQuery("INSERT INTO customers (id, id2, id3, a, b, c) VALUES (?, ?, ?, ?, ?, ?)", 1, 2, 3, 4, 5, 6);
        executeFormattedQuery("INSERT INTO customers (id, id2, id3, a, b, c) VALUES (?, ?, ?, ?, ?, ?)", 2, 3, 4, 5, 6, 7);
        executeFormattedQuery("INSERT INTO orders (id, customer_id, customer_id2, employee_id, warehouse_id) VALUES (?, ?, ?, ?, ?)", 1, 1, 2, 1, 1);
        executeFormattedQuery("INSERT INTO orders (id, customer_id, customer_id2, employee_id, warehouse_id) VALUES (?, ?, ?, ?, ?)", 2, 1, 2, 2, 2);
        executeFormattedQuery("INSERT INTO employees (id) VALUES (?)", 1);
        executeFormattedQuery("INSERT INTO employees (id) VALUES (?)", 2);
        executeFormattedQuery("INSERT INTO warehouses (id) VALUES (?)", 1);
        executeFormattedQuery("INSERT INTO warehouses (id) VALUES (?)", 2);
    }


    protected UntypedResultSet executeFormattedQuery(String query, Object... values) throws Throwable
    {
        return super.executeFormattedQuery(mapTables(query), values);
    }

    protected void assertInvalidMessage(String errorMessage, String query, Object... values) throws Throwable
    {
        try
        {
            super.assertInvalidMessage(mapTables(errorMessage), query, values);
        }
        catch (AssertionError e)
        {
            AssertionError munged = new AssertionError(restoreTables(e.getMessage()), e);
            munged.setStackTrace(e.getStackTrace());
            throw munged;
        }
    }

    private String mapTables(String string)
    {
        return string.replace("customers", customers)
                     .replace("orders", orders)
                     .replace("employees", employees)
                     .replace("warehouses", warehouses);
    }

    private String restoreTables(String string)
    {
        return string.replace(customers, "customers")
                     .replace(orders, "orders")
                     .replace(employees, "employees")
                     .replace(warehouses, "warehouses");
    }

    @Test
    public void testInnerJoinWithWhereAndParameter() throws Throwable
    {
        assertRows(executeFormattedQuery("SELECT c.id FROM customers AS c INNER JOIN orders o ON c.id = o.customer_id WHERE c.id = ? ALLOW FILTERING", 1),
                   row(1),
                   row(1));
    }

    @Test
    public void testInnerJoinWithWhereAndParameterPrepared() throws Throwable
    {
        prepare(mapTables("SELECT c.id FROM customers AS c INNER JOIN orders o ON c.id = o.customer_id WHERE c.id = ? ALLOW FILTERING"));
        assertRows(executeFormattedQuery("SELECT c.id FROM customers AS c INNER JOIN orders o ON c.id = o.customer_id WHERE c.id = ? ALLOW FILTERING", 1),
                   row(1),
                   row(1));
    }

    @Test
    public void testJoinColumnNames() throws Throwable
    {
        assertColumnNames(executeFormattedQuery("SELECT c.id, o.id FROM customers AS c INNER JOIN orders o ON c.id = o.customer_id WHERE c.id = ? ALLOW FILTERING", 1),
                          "c_id", "o_id");
    }

    @Test
    public void testJoinSumSelectionFunction() throws Throwable
    {
        assertRows(executeFormattedQuery("SELECT c.id, o.id, c.id + o.id as total FROM customers AS c INNER JOIN orders o ON c.id = o.customer_id WHERE c.id = ? ALLOW FILTERING", 1),
        row(1, 1, 2),
                   row(1, 2, 3));
    }

    @Test
    public void testInnerJoin() throws Throwable
    {
        assertRows(executeFormattedQuery("SELECT c.id FROM customers AS c INNER JOIN orders o ON c.id = o.customer_id ALLOW FILTERING"),
                   row(1),
                   row(1));
    }

    @Test
    public void testJoinWildCards() throws Throwable
    {
        assertRows(executeFormattedQuery("SELECT c.*, o.* FROM customers AS c INNER JOIN orders o ON c.id = o.customer_id ALLOW FILTERING"),
                   row(1, 2, 3, 4, 5, 6, 1, 1, 2, 1, 1),
                   row(1, 2, 3, 4, 5, 6, 2, 1, 2, 2, 2));
    }

    @Test
    public void testLeftJoin() throws Throwable
    {
        assertRows(executeFormattedQuery("SELECT c.id FROM customers AS c LEFT JOIN orders o ON c.id = o.customer_id ALLOW FILTERING"),
                   row(1),
                   row(1),
                   row(2));
    }

    @Test
    public void testLeftJoinWithFork() throws Throwable
    {
        assertRows(executeFormattedQuery("SELECT c.id, o.id, e.id FROM customers AS c LEFT JOIN orders o ON c.id = o.customer_id LEFT JOIN employees e ON e.id = c.id ALLOW FILTERING"),
                   row(1, 1, 1),
                   row(1, 2, 1),
                   row(2, null, 2));
    }


    @Test
    public void testRightJoin() throws Throwable
    {
        assertRows(executeFormattedQuery("SELECT c.id FROM orders AS o RIGHT JOIN customers c ON c.id = o.customer_id ALLOW FILTERING"),
                   row(1),
                   row(1),
                   row(2));
    }

    @Test
    public void testUndefinedColumnInJoin() throws Throwable
    {
        assertInvalidMessage("Undefined column o.customer_id_unknown in INNER JOIN orders AS o ON c.id = o.customer_id_unknown",
                             "SELECT c.id FROM customers AS c " +
                             "INNER JOIN orders o ON c.id = o.customer_id_unknown");
    }

    @Test
    public void testUndefinedTableAliasInJoin() throws Throwable
    {
        assertInvalidMessage("Undefined table alias unknown in INNER JOIN orders AS o ON c.id = unknown.id",
                             "SELECT c.id FROM customers AS c " +
                             "INNER JOIN orders o ON c.id = unknown.id");
    }

    @Test
    public void testSingleTableReferencedInJoin() throws Throwable
    {
        assertInvalidMessage("Join c.id = c.id must reference table orders AS o in INNER JOIN orders AS o ON c.id = c.id",
                             "SELECT c.id FROM customers AS c " +
                             "INNER JOIN orders o ON c.id = c.id");
    }

    @Test
    public void testMultipleTablesReferencedInJoin() throws Throwable
    {
        assertInvalidMessage("Two tables must be referenced in INNER JOIN orders AS o ON c.id = o.customer_id AND c.id = e.id",
                             "SELECT c.id FROM customers AS c " +
                             "INNER JOIN orders o ON c.id = o.customer_id AND c.id = e.id " +
                             "INNER JOIN employees e ON c.id = c.id");
    }

    @Test
    public void testKeywordSelection() throws Throwable
    {
        assertInvalidMessage("Invalid field selection: c of type int is not a user type",
                             "SELECT c.key FROM customers AS c INNER JOIN orders o ON c.id = o.customer_id ALLOW FILTERING");
    }

}
