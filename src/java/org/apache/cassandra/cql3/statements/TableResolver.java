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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Encapsulates resolution of aliased schema entities to real entities.
 */
public class TableResolver
{
    private Map<ColumnIdentifier, QualifiedName> tables;
    private Schema schema = Schema.instance;

    public TableResolver(QualifiedName primary, List<Join.Raw> joinClauses)
    {

        this.tables = new HashMap<>(joinClauses.size() + 1);
        tables.put(primary.getAlias(), primary);
        for (Join.Raw join : joinClauses)
        {
            QualifiedName joinTable = join.getTable();
            if(joinTable.getKeyspace() == null)
            {
                joinTable.setKeyspace(primary.getKeyspace(), false);
            }
            schema.validateTable(joinTable.getKeyspace(), joinTable.getName());
            QualifiedName previous = tables.put(joinTable.getAlias(), joinTable);
            if (previous != null)
            {
                throw new InvalidRequestException(
                String.format("Cannot alias table %s as %s. The same alias has previously been used for %s",
                              joinTable.getName(),
                              joinTable.getAlias(),
                              previous.getName()));
            }
        }
    }

    public QualifiedName resolveTable(ColumnIdentifier alias)
    {
        return tables.get(alias);
    }

    public TableMetadata resolveTableMetadata(ColumnIdentifier alias)
    {
        QualifiedName qualifiedName = tables.get(alias);
        if(qualifiedName == null) {
            return null;
        }
        return schema.getTableMetadata(qualifiedName.getKeyspace(), qualifiedName.getName());
    }

    public TableMetadata resolveTableMetadata(Selectable.Raw selectable)
    {
        return resolveTableMetadata(getAlias(selectable));
    }


    public TableMetadata resolveTableMetadata(RawSelector s)
    {
        return resolveTableMetadata(s.selectable);
    }

    public ColumnIdentifier getAlias(Selectable.Raw selectable)
    {
        if(selectable instanceof ColumnMetadata.Raw.Literal)
        {
            return ((ColumnMetadata.Raw.Literal) selectable).getTableAlias();
        }
        if(selectable instanceof Selectable.RawIdentifier)
        {
            return ((Selectable.RawIdentifier) selectable).getTableAlias();
        }
        if(selectable instanceof Selectable.WithFieldSelection.Raw)
        {
            //We may have a field selection, or we may have an alias.
            Selectable.Raw selected = ((Selectable.WithFieldSelection.Raw) selectable).getSelected();
            return ColumnIdentifier.getInterned(((Selectable.RawIdentifier) selected).toFieldIdentifier().toString(), true);

        }
        return null;
    }

    public ColumnMetadata resolveColumn(ColumnMetadata.Raw selectable)
    {
        TableMetadata tableMetadata = resolveTableMetadata(selectable.getTableAlias());
        return tableMetadata.getColumn(selectable.getIdentifier(tableMetadata));
    }

    public ColumnMetadata resolveColumn(Selectable.RawIdentifier selectable)
    {
        return resolveTableMetadata(selectable.getTableAlias()).getColumn(selectable.toFieldIdentifier().bytes);
    }

    public ColumnMetadata resolveColumn(Selectable.Raw selectable)
    {
        if(selectable instanceof ColumnMetadata.Raw.Literal)
        {
            return resolveColumn((ColumnMetadata.Raw.Literal) selectable);
        }
        if(selectable instanceof Selectable.RawIdentifier)
        {
            return resolveColumn((Selectable.RawIdentifier) selectable);
        }
        throw new UnsupportedOperationException("Cannot resolve selectable of type " + selectable.getClass());
    }

    public TableMetadata resolveTableMetadata(Selectable selectable)
    {
        if(selectable instanceof ColumnMetadata) {
            return schema.getTableMetadata(((ColumnMetadata) selectable).ksName, ((ColumnMetadata) selectable).cfName);
        }
        return null;
    }
}
