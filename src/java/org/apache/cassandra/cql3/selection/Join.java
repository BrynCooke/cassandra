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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import com.carrotsearch.hppc.IntArrayList;
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
    private QualifiedName table;
    private SelectStatement select;
    private int[] joinParameterIdx;
    private List<ByteBuffer> emptyResult;

    public Join(Type type,  QualifiedName table, int[] joinParameterIdx, SelectStatement select)
    {
        this.type = type;
        this.table = table;
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

    public int[] getJoinParameterIdx()
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
            return type.toString()
                       .toUpperCase() + " JOIN " + table + " ON " + joinColumns.stream()
                                                                               .map(j -> j.left + " = " + j.right)
                                                                               .collect(Collectors.joining(" AND "));
        }

        public Join prepare(TableResolver tableResolver, RawStatement raw, List<Join.Raw> unpreparedJoins, List<Join> preparedJoins)
        {
            RawStatement joinStatement = getStatement(tableResolver, raw, unpreparedJoins);

            try
            {
                int[] joinParameterIndexes = getJoinParameterIndexes(tableResolver, preparedJoins);

                return new Join(type, table, joinParameterIndexes, joinStatement.prepareRegularSelect(false));
            }
            catch (InvalidRequestException e) {
                throw new InvalidRequestException(String.format("Cannot %s. %s", this, e.getMessage()));
            }
        }

        private int[] getJoinParameterIndexes(TableResolver tableResolver, List<Join> preparedJoins)
        {
            IntArrayList params = new IntArrayList();
            if (type != Type.Primary)
            {
                for (Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns : joinColumns)
                {
                    ColumnMetadata.Raw foreignJoinColumn = getForeigenJoinColumn(columns);
                    int index = getIndex(tableResolver, preparedJoins, foreignJoinColumn);
                    params.add(index);
                }
            }
            return params.toArray();
        }

        public static int getIndex(TableResolver tableResolver, List<Join> preparedJoins, ColumnMetadata.Raw column)
        {
            int index = 0;
            ColumnMetadata resolved = tableResolver.resolveColumn(column);
            for (Join join : preparedJoins)
            {
                if(Objects.equals(join.table.getAlias(), column.getAlias())) {
                    //Not the table with the join columns, we need to skip the data entirely.
                    int i = join.select.getSelection().getColumns().indexOf(resolved);
                    Preconditions.checkState(i != -1, "Failed to find selected column %s", column);
                    index += i;
                    break;
                }
                else {
                    //Not the table with the join columns, we need to skip the data entirely.
                    index += join.select.getSelection().getColumns().size();
                }
            }
            return index;
        }

        private RawStatement getStatement(TableResolver tableResolver, RawStatement raw, List<Join.Raw> unpreparedJoins)
        {
            List<RawSelector> selectors = getSelectors(raw, unpreparedJoins);
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
            //We need the information from the left had side of the join to do the actual join.
            if (type != Type.Primary)
            {
                for (Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns : joinColumns)
                {
                    ColumnMetadata.Raw joinColumn = getJoinColumn(columns);
                    where.add(new SingleColumnRelation(joinColumn, Operator.EQ, new AbstractMarker.Raw(bindingsCount++)));
                }
            }
            RawStatement rawStatement = new RawStatement(new QualifiedName(tableResolver.getPrimary().keyspace, table.getName()),
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

        private List<RawSelector> getSelectors(RawStatement raw, List<Join.Raw> unpreparedJoins)
        {
            //We need not only the items in the select clause, but also any join clause forign keys
            HashSet<String> selectors = new LinkedHashSet<>();
            raw.selectClause.stream().filter(selector -> {
                if (selector.selectable instanceof Selectable.RawIdentifier)
                {
                    ColumnIdentifier selectableAlias = ((Selectable.RawIdentifier) selector.selectable).getAlias();
                    if(Objects.equals(selectableAlias, table.getAlias())) {
                        return true;
                    };
                }
                return false;
            }).map(s->((Selectable.RawIdentifier) s.selectable).toFieldIdentifier().toString()).forEach(selectors::add);

            unpreparedJoins.stream()
                           .flatMap(j->j.getJoinColumns().stream().map(jc->j.getForeigenJoinColumn(jc)))
                           .filter(jc->Objects.equals(jc.getAlias(), table.getAlias()))
                            .forEach(jc->selectors.add(jc.rawText()));

            return selectors.stream().map(s->new RawSelector(ColumnMetadata.Raw.forUnquoted(s), null)).collect(Collectors.toList());
        }

        private ColumnMetadata.Raw getJoinColumn(Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns)
        {
            if(Objects.equals(columns.left().getAlias(), table.getAlias())) {
                return columns.left();
            }
            return columns.right();
        }

        private ColumnMetadata.Raw getForeigenJoinColumn(Pair<ColumnMetadata.Raw, ColumnMetadata.Raw> columns)
        {
            if(Objects.equals(columns.left().getAlias(), table.getAlias())) {
                return columns.right();
            }
            return columns.left();
        }
    }
}