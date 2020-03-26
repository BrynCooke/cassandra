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

import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.MultiColumnRelation;
import org.apache.cassandra.cql3.Operator;


import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.SelectStatement.RawStatement;
import org.apache.cassandra.cql3.statements.TableResolver;
import org.apache.cassandra.exceptions.InvalidRequestException;
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
    private SelectStatement select;

    public Join(Type type, SelectStatement select)
    {
        this.type = type;
        this.select = select;
    }

    public Type getType()
    {
        return type;
    }

    public SelectStatement getSelect()
    {
        return select;
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

        public Join prepare(TableResolver tableResolver, RawStatement raw)
        {
            RawStatement joinStatement = getJoinStatement(tableResolver, raw);
            try
            {
                return new Join(type, joinStatement.prepare(false));
            }
            catch (InvalidRequestException e) {
                throw new InvalidRequestException(String.format("Cannot %s. %s", this, e.getMessage()));
            }
        }

        private RawStatement getJoinStatement(TableResolver tableResolver, RawStatement raw)
        {
            List<RawSelector> selectors = raw.selectClause.stream().filter(selector -> {
                if (selector.selectable instanceof Selectable.RawIdentifier)
                {
                    ColumnIdentifier selectableAlias = ((Selectable.RawIdentifier) selector.selectable).getAlias();
                    return selectableAlias.equals(table.getAlias());
                }
                return false;
            }).collect(Collectors.toList());

            WhereClause.Builder where = new WhereClause.Builder();
            raw.whereClause.expressions.forEach(where::add);
            for (Relation relation : raw.whereClause.relations)
            {
                if (relation instanceof SingleColumnRelation)
                {
                    SingleColumnRelation singleColumnRelation = (SingleColumnRelation) relation;
                    if (Objects.equals(table.getAlias(), singleColumnRelation.getEntity().getAlias()))
                    {
                        where.add(singleColumnRelation);
                    }
                }
                if (relation instanceof MultiColumnRelation)
                {
                    //We should have already validated that multi-column relations do not contain multiple aliases.
                    MultiColumnRelation multiColumnRelation = (MultiColumnRelation) relation;
                    if(Objects.equals(table.getAlias(), multiColumnRelation.getEntities().get(0).getAlias()))
                    {
                        where.add(multiColumnRelation);
                    }
                }
            }

            int bindingsCount = raw.getBindVariablesSize();
            //Unless we're doing a right join then we need the information from the left had side of the join.
            if (type == Type.Left || type == Type.Inner)
            {
                for (Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns : joinColumns)
                {
                    ColumnMetadata.Raw joinColumn = getJoinColumn(columns);
                    where.add(new SingleColumnRelation(joinColumn, Operator.EQ, new AbstractMarker.Raw(bindingsCount++)));
                }
            }

            RawStatement rawStatement = new RawStatement(table,
                                                         raw.parameters,
                                                         selectors,
                                                         Collections.emptyList(),
                                                         where.build(),
                                                         raw.limit,
                                                         raw.perPartitionLimit
            );

            //As bindings are positional we can safely use the bindings from the primary statement.
            //Anything that is not used by the join statement will be ignored.
            rawStatement.setBindVariables(new ArrayList<>(Collections.nCopies(bindingsCount, null)));
            return rawStatement;
        }

        private ColumnMetadata.Raw getJoinColumn(Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns)
        {
            if(Objects.equals(columns.left().getAlias(), table.getAlias())) {
                return columns.left();
            }
            return columns.right();
        }
    }
}