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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.selection.Join;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Encapsulates resolution of aliased schema entities to real entities.
 */
public class TableResolver
{
    private final TableMetadata primary;
    private Map<ColumnIdentifier, TableMetadata> tables;

    public static TableResolver ofPrimary(QualifiedName primary)
    {
        return new TableResolver(Schema.instance, primary, Collections.emptyList());
    }

    public TableResolver(Schema schema, QualifiedName primary, List<Join.Raw> joinClauses)
    {
        this.tables = new HashMap<>(joinClauses.size() + 1);
        this.primary = schema.validateTable(primary.getKeyspace(), primary.getName());
        tables.put(primary.getAlias(), this.primary);
        for (Join.Raw join : joinClauses)
        {
            QualifiedName joinTable = join.getTable();
            TableMetadata previous = tables.put(joinTable.getAlias(), schema.validateTable(joinTable.getKeyspace(), joinTable.getName()));
            if (previous != null)
            {
                throw new InvalidRequestException(
                String.format("Cannot alias table %s as %s. The same alias has previously been used for %s",
                              joinTable.getName(),
                              joinTable.getAlias(),
                              previous.name));
            }
        }
    }

    public TableMetadata resolveTable(ColumnIdentifier alias)
    {
        return tables.get(alias);
    }

    public ColumnMetadata resolveColumn(ColumnMetadata.Raw column)
    {
        TableMetadata tableMetadata = tables.get(column.getAlias());
        return tables.get(column.getAlias()).getColumn(column.getIdentifier(tableMetadata));
    }

    public TableMetadata getPrimary()
    {
        return primary;
    }

}
