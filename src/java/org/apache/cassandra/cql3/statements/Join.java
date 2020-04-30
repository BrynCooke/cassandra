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

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public final class Join
{
    public static class JoinResult extends AbstractList<ByteBuffer>
    {

        private final List<ByteBuffer> left;
        private final List<ByteBuffer> right;

        public JoinResult(List<ByteBuffer> left, List<ByteBuffer> right)
        {
            this.left = left;
            this.right = right;
        }

        public ByteBuffer get(int index)
        {
            if (index < left.size())
            {
                return left.get(index);
            }
            return right.get(index - left.size());
        }

        public int size()
        {
            return left.size() + right.size();
        }
    }

    public enum Type
    {
        Primary,
        Inner,
        Left,
        Right
    }

    private final Type type;
    private QualifiedName table;
    private int[] joinParameterPartitionIdx;
    private int[] joinParameterClusteringIdx;
    private int[] joinResultClusteringIdx;
    private SelectStatement select;
    private Raw raw;
    private SelectStatement.RawStatement rawStatement;
    private int[] joinParameterIdx;
    private List<ByteBuffer> emptyResult;

    public Join(Type type, QualifiedName table, Raw raw, SelectStatement.RawStatement rawStatement, int[] joinParameterIdx, int[] joinParameterPartitionIdx, int[] joinParameterClusteringIdx, int[] joinResultClusteringIdx, SelectStatement select)
    {
        this.type = type;
        this.table = table;
        this.raw = raw;
        this.rawStatement = rawStatement;
        this.joinParameterIdx = joinParameterIdx;
        this.joinParameterPartitionIdx = joinParameterPartitionIdx;
        this.joinParameterClusteringIdx = joinParameterClusteringIdx;
        this.joinResultClusteringIdx = joinResultClusteringIdx;
        this.select = select;
        emptyResult = Collections.nCopies(select.getSelection().getColumns().size(), null);
    }

    public Type getType()
    {
        return type;
    }

    public SelectStatement getSelect()
    {
        return select;
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Raw getRaw()
    {
        return raw;
    }

    public SelectStatement.RawStatement getRawStatement()
    {
        return rawStatement;
    }

    public int[] getJoinMapping()
    {
        return joinParameterIdx;
    }

    public int[] getJoinParameterPartitionIdx()
    {
        return joinParameterPartitionIdx;
    }

    public int[] getJoinParameterClusteringIdx()
    {
        return joinParameterClusteringIdx;
    }
    public int[] getJoinResultClusteringIdx()
    {
        return joinResultClusteringIdx;
    }

    public List<ByteBuffer> getEmptyResult()
    {
        return emptyResult;
    }

    public static final class Raw
    {
        private final Type type;
        private final QualifiedName table;
        private List<Pair<ColumnMetadata.Raw, ColumnMetadata.Raw>> joinColumns;

        public Raw(Type type, QualifiedName table, List<Pair<ColumnMetadata.Raw, ColumnMetadata.Raw>> joinColumns)
        {
            this.type = type;
            this.table = table;
            this.joinColumns = joinColumns;
        }

        public QualifiedName getTable()
        {
            return table;
        }


        public Type getType()
        {
            return type;
        }

        public List<Pair<ColumnMetadata.Raw, ColumnMetadata.Raw>> getJoinColumns()
        {
            return joinColumns;
        }

        public String toString()
        {
            if (type == Type.Primary)
            {
                return "SELECT " + table;
            }
            return type.toString()
                       .toUpperCase() + " JOIN " + table + " ON " + joinColumns.stream()
                                                                               .map(j -> j.left + " = " + j.right)
                                                                               .collect(Collectors.joining(" AND "));
        }

        public ColumnMetadata.Raw getJoinColumn(Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns)
        {
            if (Objects.equals(columns.left().getTableAlias(), table.getAlias()))
            {
                return columns.left();
            }
            return columns.right();
        }

        public ColumnMetadata.Raw getForeigenJoinColumn(Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns)
        {
            if (Objects.equals(columns.left().getTableAlias(), table.getAlias()))
            {
                return columns.right();
            }
            return columns.left();
        }

        public Join.Raw asPrimary()
        {
            return new Join.Raw(Type.Primary, table, Collections.emptyList());
        }

        public Raw invert(Raw join, Type type)
        {
            return new Join.Raw(type, join.table, joinColumns);
        }

        public void validate(TableResolver tableResolver)
        {

            Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> first = getJoinColumns().get(0);
            for (Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> joinColumn : getJoinColumns())
            {
                checkNotNull(tableResolver.resolveTableMetadata(joinColumn.left().getTableAlias()),
                             "Undefined table alias %s in %s", joinColumn.left().getTableAlias(), this);
                checkNotNull(tableResolver.resolveTableMetadata(joinColumn.right().getTableAlias()),
                             "Undefined table alias %s in %s", joinColumn.right().getTableAlias(), this);
                checkNotNull(tableResolver.resolveColumn(joinColumn.left()),
                             "Undefined column %s in %s", joinColumn.left(), this);
                checkNotNull(tableResolver.resolveColumn(joinColumn.right()),
                             "Undefined column %s in %s", joinColumn.right(), this);
                checkTrue(Objects.equals(joinColumn.left().getTableAlias(), first.left().getTableAlias()) &&
                          Objects.equals(joinColumn.right().getTableAlias(), first.right().getTableAlias()) ||
                          Objects.equals(joinColumn.left().getTableAlias(), first.right().getTableAlias()) &&
                          Objects.equals(joinColumn.right().getTableAlias(), first.left().getTableAlias()),
                          "Two tables must be referenced in %s", this);
                checkTrue(Objects.equals(joinColumn.left().getTableAlias(), getTable().getAlias())
                          || Objects.equals(joinColumn.right().getTableAlias(), getTable().getAlias()),
                          "Join %s = %s must reference table %s in %s", joinColumn.left(), joinColumn.right(), getTable(), this);
            }
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Raw raw = (Raw) o;
            return type == raw.type &&
                   Objects.equals(table, raw.table) &&
                   Objects.equals(joinColumns, raw.joinColumns);
        }

        public int hashCode()
        {
            return Objects.hash(type, table, joinColumns);
        }
    }
}