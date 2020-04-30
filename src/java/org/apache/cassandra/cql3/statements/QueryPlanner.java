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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.MultiColumnRelation;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.Join.Type.Inner;
import static org.apache.cassandra.cql3.statements.Join.Type.Left;
import static org.apache.cassandra.schema.ColumnMetadata.Raw.forQuoted;

/**
 * Takes a select statement with joins and creates a query plan composed of a list of select statements that can be
 * looped over.
 */
public class QueryPlanner
{

    public static class QueryPlan
    {
        private List<Join> joins;
        private List<Selectable> selectables;
        private VariableSpecifications boundNames;
        private Set<ColumnMetadata> orderingColumns;
        private Set<ColumnMetadata> nonPKRestrictedColumns;
        private boolean hasGroupBy;
        private boolean isJson;
        private List<Selectable.Raw> joinRow;
        private final List<ColumnMetadata> joinRowColumnMetadata;

        public QueryPlan(TableResolver tableResolver,
                         List<Selectable.Raw> joinRow,
                         List<Join> joins,
                         List<Selectable> selectables,
                         VariableSpecifications boundNames,
                         Set<ColumnMetadata> orderingColumns,
                         Set<ColumnMetadata> nonPKRestrictedColumns,
                         boolean hasGroupBy,
                         boolean isJson)
        {
            this.joinRow = joinRow;
            this.joins = joins;
            this.selectables = selectables;
            this.boundNames = boundNames;
            this.orderingColumns = orderingColumns;
            this.nonPKRestrictedColumns = nonPKRestrictedColumns;
            this.hasGroupBy = hasGroupBy;
            this.isJson = isJson;
            joinRowColumnMetadata = joinRow.stream().map(c -> tableResolver.resolveColumn(c)).collect(Collectors.toList());
        }

        public List<Selectable.Raw> getJoinRow()
        {
            return joinRow;
        }

        public List<Join> getJoins()
        {
            return Collections.unmodifiableList(joins);
        }

        public List<Selectable> getSelectables()
        {
            return selectables;
        }

        public boolean isJson()
        {
            return isJson;
        }

        public TableMetadata getPrimaryTable()
        {
            return joins.get(0).getSelect().table;
        }

        public Set<ColumnMetadata> getNonPKRestrictedColumns()
        {
            return nonPKRestrictedColumns;
        }

        public Set<ColumnMetadata> getOrderingColumns()
        {
            return orderingColumns;
        }

        public VariableSpecifications getBoundNames()
        {
            return boundNames;
        }

        public Function<List<ByteBuffer>, List<ByteBuffer>> getResultMapping(Selection selection)
        {
            int[] resultMapping = selection.getColumns().stream().mapToInt(c -> joinRowColumnMetadata.indexOf(c)).toArray();
            return source -> new AbstractList<ByteBuffer>()
            {

                public int size()
                {
                    return resultMapping.length;
                }

                public ByteBuffer get(int index)
                {
                    return source.get(resultMapping[index]);
                }
            };
        }
    }

    private final TableResolver tableResolver;
    private SelectStatement.RawStatement raw;

    public QueryPlanner(SelectStatement.RawStatement raw)
    {
        this.tableResolver = new TableResolver(raw.qualifiedName, raw.joinClauses);
        this.raw = raw;
    }

    public QueryPlan prepare()
    {
        List<Join.Raw> joins = reorderJoins();
        List<SelectStatement.RawStatement> statements = selectStatements(joins);
        List<Selectable.Raw> joinRow = new ArrayList<>();
        List<Join> preparedJoins = new ArrayList<>();
        for (int count = 0; count < statements.size(); count++)
        {
            Join.Raw join = joins.get(count);
            SelectStatement.RawStatement rawStatement = statements.get(count);
            SelectStatement subSelect = rawStatement.prepare(false);
            joinRow.addAll(subSelect.getSelection().getColumnMapping().getColumnSpecifications().stream()
                                    .map(spec -> forQuoted(spec.name.toString(), join.getTable().getAlias()))
                                    .collect(Collectors.toList()));
            int[] parameterIndexes = join.getJoinColumns().stream()
                                         .map(jc -> join.getForeigenJoinColumn(jc))
                                         .mapToInt(joinRow::indexOf)
                                         .toArray();
            int[] partitionParameterIndexes = join.getJoinColumns().stream()
                                         .filter(jc->tableResolver.resolveColumn(join.getJoinColumn(jc)).isPartitionKey())
                                         .map(jc -> join.getForeigenJoinColumn(jc))
                                         .mapToInt(joinRow::indexOf)
                                         .toArray();
            int[] clusteringParameterIndexes = join.getJoinColumns().stream()
                                                  .filter(jc->tableResolver.resolveColumn(join.getJoinColumn(jc)).isClusteringColumn())
                                                  .map(jc -> join.getForeigenJoinColumn(jc))
                                                  .mapToInt(joinRow::indexOf)
                                                  .toArray();
            int[] joinResultClusteringIdx = join.getJoinColumns().stream()
                                                   .filter(jc->tableResolver.resolveColumn(join.getJoinColumn(jc)).isClusteringColumn())
                                                   .map(jc -> join.getJoinColumn(jc))
                                                   .mapToInt(joinRow::indexOf)
                                                   .toArray();
            preparedJoins.add(new Join(join.getType(),
                                       join.getTable(),
                                       join,
                                       rawStatement,
                                       parameterIndexes,
                                       partitionParameterIndexes,
                                       clusteringParameterIndexes,
                                       joinResultClusteringIdx,
                                       subSelect));
        }
        VariableSpecifications boundVariables = getBoundVariables(preparedJoins);
        List<Selectable> selectables = getSelectables();
        return new QueryPlan(tableResolver,
                             joinRow,
                             preparedJoins,
                             selectables,
                             boundVariables,
                             Collections.emptySet(),
                             Collections.emptySet(),
                             !raw.parameters.groups.isEmpty(),
                             raw.parameters.isJson);
    }

    private VariableSpecifications getBoundVariables(List<Join> preparedJoins)
    {
        VariableSpecifications variableSpecifications = new VariableSpecifications(Collections.nCopies(raw.getBindVariables().getBindVariables().size(), null));
        preparedJoins.stream().map(j -> j.getSelect().bindVariables).forEach(b -> {
            for (int count = 0; count < raw.getBindVariables().getBindVariables().size(); count++)
            {
                ColumnSpecification columnSpecification = b.getBindVariables().get(count);
                if (columnSpecification != null)
                {
                    variableSpecifications.add(count, columnSpecification);
                }
            }
        });
        return variableSpecifications;
    }

    private List<Selectable> getSelectables()
    {
        return raw.selectClause.stream()
                               .flatMap(s -> {
                                   if (s.selectable instanceof Selectable.RawIdentifier)
                                   {
                                       Selectable.RawIdentifier selectable = (Selectable.RawIdentifier) s.selectable;
                                       if (((Selectable.RawIdentifier) s.selectable).isWildCard())
                                       {
                                           TableMetadata tableMetadata = tableResolver.resolveTableMetadata(selectable.getTableAlias());
                                           return Lists.newArrayList(tableMetadata.allColumnsInSelectOrder())
                                                       .stream()
                                                       .map(c -> new RawSelector(forQuoted(c.name.toString(), selectable.getTableAlias()), getAlias(c.name.toString(), selectable.getTableAlias())));
                                       }
                                       else if (s.alias == null && ((Selectable.RawIdentifier) s.selectable).getTableAlias() != null)
                                       {
                                           return Stream.of(new RawSelector(s.selectable, getAlias(selectable.toFieldIdentifier().toString(), selectable.getTableAlias())));
                                       }
                                   }

                                   return Stream.of(s);
                               })
                               .map(s -> {
                                   TableMetadata tableMetadata = tableResolver.resolveTableMetadata(s);
                                   if (tableMetadata == null)
                                   {
                                       tableMetadata = tableResolver.resolveTableMetadata((ColumnIdentifier) null);
                                   }
                                   if (tableMetadata == null)
                                   {
                                       tableMetadata = tableResolver.resolveTableMetadata(raw.qualifiedName);
                                   }

                                   return s.prepare(tableMetadata, tableResolver);
                               })
                               .collect(Collectors.toList());
    }

    private ColumnIdentifier getAlias(String raw, ColumnIdentifier tableAlias)
    {
        if (tableAlias == null)
        {
            return ColumnIdentifier.getInterned(raw, true);
        }
        return ColumnIdentifier.getInterned(tableAlias.toString() + "_" + raw, true);
    }

    private List<SelectStatement.RawStatement> selectStatements(List<Join.Raw> joins)
    {
        List<SelectStatement.RawStatement> statements = joins.stream().map(j -> {
            //Selectables are composed of those that are specified in the raw statement, and those that are required by joins.
            Set<Selectable.Raw> selectables = new LinkedHashSet<>();
            selectables.addAll(rawStatementSelectables(j.getTable()));
            selectables.addAll(rawStatementJoinSelectables(j.getTable()));
            List<RawSelector> selectors = selectables.stream().map(s -> new RawSelector(s, null)).collect(Collectors.toList());

            //Wheres are copied from the raw statment and by the join columns
            WhereClause.Builder where = new WhereClause.Builder();
            rawStatementWheres(j.getTable()).forEach(where::add);
            joinWheres(j).forEach(where::add);
            SelectStatement.RawStatement rawStatement = new SelectStatement.RawStatement(j.getTable(),
                                                                                         raw.parameters,
                                                                                         selectors,
                                                                                         Collections.emptyList(),
                                                                                         where.build(),
                                                                                         raw.limit,
                                                                                         raw.perPartitionLimit);
            rawStatement.setBindVariables(Collections.nCopies(raw.getBindVariables().getBindVariables().size() + j.getJoinColumns().size(), null));
            return rawStatement;
        }).collect(Collectors.toList());
        return statements;
    }

    private List<Relation> joinWheres(Join.Raw join)
    {
        AtomicInteger bindingIndex = new AtomicInteger(raw.getBindVariables().getBindVariables().size());
        return join.getJoinColumns()
                   .stream()
                   .map(jc -> {

                       ColumnMetadata.Raw joinColumn = join.getJoinColumn(jc);
                       ColumnMetadata columnMetadata = tableResolver.resolveColumn(joinColumn);
                       boolean clustering = columnMetadata.isClusteringColumn();
                       return new SingleColumnRelation(join.getJoinColumn(jc),
                                                       clustering ? Operator.IN : Operator.EQ,
                                                       clustering ? new AbstractMarker.INRaw(bindingIndex.getAndIncrement())
                                                                  : new AbstractMarker.Raw(bindingIndex.getAndIncrement()));
                   })
                   .collect(Collectors.toList());
    }

    private Collection<? extends Selectable.Raw> rawStatementJoinSelectables(QualifiedName table)
    {
        return raw.joinClauses.stream()
                              .flatMap(j -> j.getJoinColumns().stream())
                              .flatMap(jc -> Stream.of(jc.left(), jc.right()))
                              .filter(jc -> Objects.equals(jc.getTableAlias(), table.getAlias()))
                              .map(jc -> Selectable.RawIdentifier.forUnquoted(jc.rawText(), jc.getTableAlias()))
                              .collect(Collectors.toList());
    }

    private List<Relation> rawStatementWheres(QualifiedName table)
    {

        List<Relation> relations = raw.whereClause.relations;
        return relations.stream()
                        .filter(w -> {
                            if (w instanceof SingleColumnRelation)
                            {
                                return Objects.equals(((SingleColumnRelation) w).getEntity().getTableAlias(), table.getAlias());
                            }
                            else if (w instanceof MultiColumnRelation)
                            {
                                return Objects.equals(((MultiColumnRelation) w).getEntities().get(0).getTableAlias(), table.getAlias());
                            }
                            throw new IllegalStateException("Unknown relation type");
                        })
                        .collect(Collectors.toList());
    }

    private List<Selectable.Raw> rawStatementSelectables(QualifiedName table)
    {
        List<Selectable.Raw> rawIdentifiers = new ArrayList<>();
        raw.selectClause.forEach(sc -> visitSelectables(sc.selectable, s -> {
            if (s instanceof Selectable.RawIdentifier)
            {
                if (Objects.equals(((Selectable.RawIdentifier) s).getTableAlias(), table.getAlias()))
                {
                    rawIdentifiers.add(s);
                }
            }
            else
            {
                throw new UnsupportedOperationException("Cannot yet deal with " + s.getClass());
            }
        }));
        return rawIdentifiers;
    }

    private void visitSelectables(Selectable.Raw selectable, Consumer<Selectable.Raw> visitor)
    {
        if (selectable instanceof Selectable.RawIdentifier)
        {
            visitor.accept(selectable);
        }
        else if (selectable instanceof Selectable.WithCast.Raw)
        {
            visitSelectables(((Selectable.WithCast.Raw) selectable).getArg(), visitor);
        }
        else if (selectable instanceof Selectable.WithFunction.Raw)
        {
            ((Selectable.WithFunction.Raw) selectable).getArgs().forEach(s -> visitSelectables(s, visitor));
        }
        else if (selectable instanceof Selectable.BetweenParenthesesOrWithTuple.Raw)
        {
            ((Selectable.BetweenParenthesesOrWithTuple.Raw) selectable).getRaws().forEach(s -> visitSelectables(s, visitor));
        }
        else if (selectable instanceof Selectable.WithTerm.Raw)
        {
            visitor.accept(selectable);
        }
        else if (selectable instanceof Selectable.WithTypeHint.Raw)
        {
            visitor.accept(selectable);
        }
    }

    /**
     * @return The list of joins that can be composed in to a chain of selects. All right joins are converted to left.
     */
    private List<Join.Raw> reorderJoins()
    {
        List<Join.Raw> input = new ArrayList<>(raw.joinClauses);
        input.add(0, new Join.Raw(Join.Type.Primary, raw.qualifiedName, Collections.emptyList()));
        List<Join.Raw> output = new ArrayList<>(input.size());
        for (Join.Raw joinClause : input)
        {
            if (joinClause.getType() != Join.Type.Right)
            {
                output.add(joinClause);
            }
            else
            {
                //Right joins have to be converted in to left joins.
                Pair<List<Join.Raw>, List<Join.Raw>> split = split(output, joinClause);
                List<Join.Raw> inverted = invert(split.left, joinClause);
                output.clear();
                output.add(joinClause.asPrimary()); //This join clause becomes the new primary
                output.addAll(inverted); //The reversed path needs to happen next.
                output.addAll(split.right); //Then the rest of the existing joins can take place.
            }
        }
        return output;
    }

    /**
     * Invertes the list of joins so that they are attempted from the reverse direction.
     */
    private List<Join.Raw> invert(List<Join.Raw> joins, Join.Raw primary)
    {
        if (joins.isEmpty())
        {
            return joins;
        }
        List<Join.Raw> result = new ArrayList<>(joins.size());
        Join.Raw current = joins.get(0);
        for (Join.Raw join : joins)
        {
            if (current != join)
            {
                result.add(join.invert(current, Inner));
                current = join;
            }
        }
        result.add(primary.invert(current, Left));
        return Lists.reverse(result);
    }

    /**
     * Splits the list of joins in to the list of the joins that are in the direct path to the join supplied
     * and the joins that are not.
     * Ordering is mainteined.
     *
     * @return A pair where the left contains the direct path to the root and the right is the rest of the joins.
     */
    private Pair<List<Join.Raw>, List<Join.Raw>> split(List<Join.Raw> joins, Join.Raw join)
    {
        QualifiedName nextTable = forignTable(join);
        List<Join.Raw> left = new ArrayList<>(joins.size());
        List<Join.Raw> right = new ArrayList<>(joins.size());
        for (ListIterator<Join.Raw> it = joins.listIterator(joins.size()); it.hasPrevious(); )
        {
            Join.Raw previous = it.previous();
            if (previous.getTable().equals(nextTable))
            {
                left.add(0, previous);
                nextTable = forignTable(previous);
            }
            else
            {
                right.add(0, previous);
            }
        }
        return Pair.create(left, right);
    }

    private QualifiedName forignTable(Join.Raw join)
    {
        if (join.getType() == Join.Type.Primary)
        {
            return null;
        }
        ColumnMetadata.Raw foreigenJoinColumn = join.getForeigenJoinColumn(join.getJoinColumns().get(0));
        return tableResolver.resolveTable(foreigenJoinColumn.getTableAlias());
    }
}
