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

package org.apache.cassandra.cql3.selection;

import java.util.ArrayList;
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
        Inner,
        Left,
        Right
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
            return type.toString()
                       .toUpperCase() + " JOIN " + table + " ON " + joinColumns.stream()
                                                                               .map(j -> j.left + " = " + j.right)
                                                                               .collect(Collectors.joining(" AND "));
        }
    }
}