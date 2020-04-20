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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;

public final class Join
{

    public enum Type
    {
        Primary,
        Inner,
        Left,
        Right
    }

    private final Type type;
    private QualifiedName table;
    private SelectStatement select;
    private Raw raw;
    private SelectStatement.RawStatement rawStatement;
    private int[] joinParameterIdx;
    private List<ByteBuffer> emptyResult;

    public Join(Type type, QualifiedName table, Raw raw, SelectStatement.RawStatement rawStatement, int[] joinParameterIdx, SelectStatement select)
    {
        this.type = type;
        this.table = table;
        this.raw = raw;
        this.rawStatement = rawStatement;
        this.joinParameterIdx = joinParameterIdx;
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
            if(type == Type.Primary) {
                return "SELECT " + table;
            }
            return type.toString()
                       .toUpperCase() + " JOIN " + table + " ON " + joinColumns.stream()
                                                                               .map(j -> j.left + " = " + j.right)
                                                                               .collect(Collectors.joining(" AND "));
        }

        public ColumnMetadata.Raw getJoinColumn(Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns)
        {
            if(Objects.equals(columns.left().getTableAlias(), table.getAlias())) {
                return columns.left();
            }
            return columns.right();
        }

        public ColumnMetadata.Raw getForeigenJoinColumn(Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns)
        {
            if(Objects.equals(columns.left().getTableAlias(), table.getAlias())) {
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