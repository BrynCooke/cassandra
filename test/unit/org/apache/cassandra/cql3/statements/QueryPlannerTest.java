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

package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.cql3.statements.Join.Type.Inner;
import static org.apache.cassandra.cql3.statements.Join.Type.Left;
import static org.apache.cassandra.cql3.statements.Join.Type.Primary;

public class QueryPlannerTest
{

    private static final String KEYSPACE = "ks";

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.customers (id int, id2 int, id3 int, a int, b int, c int, primary key (id, id2, id3))");
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.orders (id int, customer_id int, customer_id2 int, employee_id int, warehouse_id int, primary key (id))");
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.employees (id int, primary key (id))");
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.warehouses (id int, primary key (id))");

        QueryProcessor.executeOnceInternal("CREATE TABLE ks.p_customers (partition int, id int, primary key (partition, id))");
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.p_orders (partition int, customer_id int, id int, primary key (partition, customer_id, id))");

    }

    @Test
    public void testPartitionedJoin()
    {
        assertThat("SELECT c.*, o.* FROM ks.p_customers c INNER JOIN ks.p_orders o ON o.partition = c.partition AND o.customer_id = c.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.p_customers c").withSelectors("c.*", "c.partition", "c.id")
        .innerJoin("ks.p_orders o", "o.partition = c.partition AND o.customer_id = c.id")
        .withWhere("o.partition = ?", "o.customer_id IN ?").withForignKeys("c.partition", "c.id")
        .withNoOtherJoins();
    }


    @Test
    public void testBasicSelect()
    {
        assertThat("SELECT * FROM ks.customers")
        .evaluatesToPlan()
        .select("ks.customers").withSelectors("*")
        .withNoOtherJoins();
    }

    @Test
    public void testInnerJoin()
    {

        assertThat("SELECT c.*, o.* FROM ks.customers c INNER JOIN ks.orders o ON o.customer_id = c.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers c").withSelectors("c.*", "c.id")
        .innerJoin("ks.orders o", "o.customer_id = c.id").withWhere("o.customer_id = ?").withForignKeys("c.id")
        .withNoOtherJoins();
    }

    @Test
    public void testMultipleInnerJoin()
    {
        assertThat("SELECT c.* FROM ks.customers c INNER JOIN ks.orders o ON o.customer_id = c.id INNER JOIN ks.employees e ON o.employee_id = e.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers c").withSelectors("c.*", "c.id")
        .innerJoin("ks.orders o", "o.customer_id = c.id").withSelectors("o.customer_id", "o.employee_id").withForignKeys("c.id")
        .innerJoin("ks.employees e", "o.employee_id = e.id").withSelectors("e.id").withForignKeys("o.employee_id")
        .withNoOtherJoins();
    }

    @Test
    public void testMultipleJoinColumns()
    {
        assertThat("SELECT c.* FROM ks.customers c INNER JOIN ks.orders o ON o.customer_id = c.id AND o.customer_id2 = c.id2 ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers c").withSelectors("c.*", "c.id", "c.id2")
        .innerJoin("ks.orders o", "o.customer_id = c.id AND o.customer_id2 = c.id2").withSelectors("o.customer_id", "o.customer_id2").withForignKeys("c.id", "c.id2")
        .withNoOtherJoins();
    }

    @Test
    public void testLeftJoin()
    {
        assertThat("SELECT c.* FROM ks.customers c LEFT JOIN ks.orders o ON o.customer_id = c.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers c").withSelectors("c.*", "c.id")
        .leftJoin("ks.orders o", "o.customer_id = c.id").withSelectors("o.customer_id").withForignKeys("c.id")
        .withNoOtherJoins();
    }

    @Test
    public void testRightJoin()
    {
        assertThat("SELECT c.* FROM ks.customers c RIGHT JOIN ks.orders o ON o.customer_id = c.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.orders o").withSelectors("o.customer_id")
        .leftJoin("ks.customers c", "o.customer_id = c.id").withSelectors("c.*", "c.id").withForignKeys("o.customer_id")
        .withNoOtherJoins();
    }

    @Test
    public void testInnerRightJoin()
    {
        assertThat("SELECT c.* FROM ks.customers c INNER JOIN ks.orders o ON o.customer_id = c.id RIGHT JOIN ks.employees e ON o.employee_id = e.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.employees e").withSelectors("e.id")
        .leftJoin("ks.orders o", "o.employee_id = e.id").withSelectors("o.employee_id", "o.customer_id").withForignKeys("e.id")
        .innerJoin("ks.customers c", "o.customer_id = c.id").withSelectors("c.*", "c.id").withForignKeys("o.customer_id")
        .withNoOtherJoins();
    }

    @Test
    public void testLeftRightJoin()
    {
        assertThat("SELECT c.* FROM ks.customers c LEFT JOIN ks.orders o ON o.customer_id = c.id RIGHT JOIN ks.employees e ON o.employee_id = e.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.employees e").withSelectors("e.id")
        .leftJoin("ks.orders o", "o.employee_id = e.id").withSelectors("o.employee_id", "o.customer_id").withForignKeys("e.id")
        .innerJoin("ks.customers c", "o.customer_id = c.id").withSelectors("c.*", "c.id").withForignKeys("o.customer_id")
        .withNoOtherJoins();
    }

    @Test
    public void testLeftLeftRightJoin()
    {
        assertThat("SELECT c.* FROM ks.customers c LEFT JOIN ks.orders o ON o.customer_id = c.id LEFT JOIN ks.warehouses w ON o.warehouse_id = w.id RIGHT JOIN ks.employees e ON o.employee_id = e.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.employees e").withSelectors("e.id")
        .leftJoin("ks.orders o", "o.employee_id = e.id").withSelectors("o.employee_id", "o.customer_id", "o.warehouse_id").withForignKeys("e.id")
        .innerJoin("ks.customers c", "o.customer_id = c.id").withSelectors("c.*", "c.id").withForignKeys("o.customer_id")
        .leftJoin("ks.warehouses w", "o.warehouse_id = w.id").withSelectors("w.id").withForignKeys("o.warehouse_id")
        .withNoOtherJoins();
    }

    @Test
    public void testRightLeftRightJoin()
    {
        assertThat("SELECT c.* FROM ks.customers c RIGHT JOIN ks.orders o ON o.customer_id = c.id LEFT JOIN ks.warehouses w ON o.warehouse_id = w.id RIGHT JOIN ks.employees e ON o.employee_id = e.id ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.employees e").withSelectors("e.id")
        .leftJoin("ks.orders o", "o.employee_id = e.id").withSelectors("o.employee_id", "o.customer_id", "o.warehouse_id").withForignKeys("e.id")
        .leftJoin("ks.customers c", "o.customer_id = c.id").withSelectors("c.*", "c.id").withForignKeys("o.customer_id")
        .leftJoin("ks.warehouses w", "o.warehouse_id = w.id").withSelectors("w.id").withForignKeys("o.warehouse_id")
        .withNoOtherJoins();
    }

    @Test
    public void testWildcardSelector()
    {
        assertThat("SELECT * FROM ks.customers ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers").withSelectors("*")
        .withNoOtherJoins();
    }

    @Test
    public void testLiteralSelector()
    {
        assertThat("SELECT a, c FROM ks.customers ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers").withSelectors("a", "c")
        .withNoOtherJoins();
    }

    @Test
    public void testFunctionSelector()
    {
        assertThat("SELECT a * (b + c) FROM ks.customers ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers").withSelectors("a", "b", "c")
        .withNoOtherJoins();
    }

    @Test
    public void testWhereConditionSingleRelation()
    {
        assertThat("SELECT * FROM ks.customers WHERE a = 1 ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers").withSelectors("*").withWhere("a = 1")
        .withNoOtherJoins();
    }

    @Test
    public void testWhereConditionAnd()
    {
        assertThat("SELECT * FROM ks.customers WHERE a = 1 AND b = 2 ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers").withSelectors("*").withWhere("a = 1", "b = 2")
        .withNoOtherJoins();
    }

    @Test
    public void testWhereConditionMultiRelation()
    {
        assertThat("SELECT * FROM ks.customers WHERE (id2, id3) IN ? ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers").withSelectors("*").withWhere("(id2, id3) IN ?")
        .withNoOtherJoins();
    }

    @Test
    public void testInnerJoinWithWhere()
    {

        assertThat("SELECT c.*, o.* FROM ks.customers c INNER JOIN ks.orders o ON o.customer_id = c.id WHERE c.a = 3 AND o.employee_id = 4 AND o.warehouse_id = ? ALLOW FILTERING")
        .evaluatesToPlan()
        .select("ks.customers c").withSelectors("c.*", "c.id").withWhere("c.a = 3")
        .innerJoin("ks.orders o", "o.customer_id = c.id").withWhere("o.customer_id = ?", "o.employee_id = 4", "o.warehouse_id = ?").withForignKeys("c.id")
        .withNoOtherJoins();
    }

    private Assertions assertThat(String query)
    {
        return new Assertions(query);
    }

    private static class Assertions extends org.assertj.core.api.Assertions
    {
        private final QueryPlanner queryPlanner;
        private List<Join> joins;
        private QueryPlanner.QueryPlan plan;

        private Join currentJoin;

        public Assertions(String query)
        {
            SelectStatement.RawStatement raw = parse(query);
            queryPlanner = new QueryPlanner(raw);
        }

        private SelectStatement.RawStatement parse(String query)
        {
            return (SelectStatement.RawStatement) QueryProcessor.parseStatement(query);
        }

        public Assertions evaluatesToPlan()
        {
            plan = queryPlanner.prepare();
            joins = new ArrayList<>(plan.getJoins());
            return this;
        }

        private Join nextJoin()
        {
            currentJoin = joins.remove(0);
            return currentJoin;
        }

        public Assertions select(String table)
        {
            checkMoreJoins();
            nextJoin();
            assertThat(currentJoin.getRaw()).isEqualTo(new Join.Raw(Primary, qName(table), Collections.emptyList()));
            return this;
        }

        public Assertions innerJoin(String table, String joinColumns)
        {
            checkMoreJoins();
            nextJoin();
            assertThat(currentJoin.getRaw()).isEqualTo(constructJoin(Inner, table, joinColumns));
            return this;
        }

        public Assertions leftJoin(String table, String joinColumns)
        {
            checkMoreJoins();
            nextJoin();
            assertThat(currentJoin.getRaw()).isEqualTo(constructJoin(Left, table, joinColumns));
            return this;
        }

        public Assertions withNoOtherJoins()
        {
            assertThat(joins).withFailMessage("Expected no more joins but got %s", joins).isEmpty();
            return this;
        }

        private Assertions checkMoreJoins()
        {
            assertThat(joins).withFailMessage("evaluatesToPlan must be called before asserting joins").isNotNull();
            assertThat(joins).isNotEmpty();
            return this;
        }

        private QualifiedName qName(String name)
        {
            String keyspace = null;
            String table;
            String alias = null;

            if (name.contains(" "))
            {
                String[] s = name.split(" ");
                name = s[0];
                alias = s[1];
            }
            if (name.contains("."))
            {
                String[] s = name.split("\\.");
                keyspace = s[0];
                table = s[1];
            }
            else
            {
                table = name;
            }
            if (alias != null)
            {
                return new QualifiedName(keyspace, table, alias);
            }
            else
            {
                return new QualifiedName(keyspace, table);
            }
        }

        private Join.Raw constructJoin(Join.Type joinType, String table, String joinColumns)
        {
            SelectStatement.RawStatement parse = parse("SELECT * FROM " + table + " " + joinType + " JOIN " + table + " ON " + joinColumns);
            return new Join.Raw(joinType, parse.joinClauses.get(0).getTable(), parse.joinClauses.get(0).getJoinColumns());
        }

        public Assertions withSelectors(String... selectors)
        {
            assertThat(currentJoin.getRawStatement().selectClause.stream().map(s -> s.selectable.toString()))
            .containsExactlyInAnyOrder(selectors);
            return this;
        }

        public Assertions withWhere(String... wheres)
        {
            assertThat(currentJoin.getRawStatement().whereClause.relations.stream().map(Object::toString).collect(Collectors.toList())).containsExactlyInAnyOrder(wheres);
            return this;
        }

        public Assertions withForignKeys(String... forignKeys)
        {
            List<String> joinColumns = Arrays.stream(currentJoin.getJoinMapping()).mapToObj(i -> plan.getJoinRow().get(i)).map(Objects::toString).collect(Collectors.toList());
            assertThat(joinColumns).containsExactly(forignKeys);
            return this;
        }
    }
}
