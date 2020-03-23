/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3.selection;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.ColumnIdentifier;

import static org.apache.cassandra.cql3.selection.Selectable.RawIdentifier.forQuoted;


public class RawSelector
{
    public final Selectable.Raw selectable;
    public final ColumnIdentifier alias;

    public RawSelector(Selectable.Raw selectable, ColumnIdentifier alias)
    {
        this.selectable = selectable;
        this.alias = alias;
    }

    /**
     * Converts the specified list of <code>RawSelector</code>s into a list of <code>Selectable</code>s.
     *
     * @param raws the <code>RawSelector</code>s to converts.
     * @return a list of <code>Selectable</code>s
     */
    public static List<Selectable> toSelectables(List<RawSelector> raws, final TableMetadata table)
    {
        return raws.stream()
                   .flatMap(raw -> {
                       if (raw.selectable instanceof Selectable.RawIdentifier
                           && ((Selectable.RawIdentifier) raw.selectable).isWildCard())
                       {
                           Selectable.RawIdentifier selectable = (Selectable.RawIdentifier) raw.selectable;
                           ArrayList<ColumnMetadata> columnMetadata = Lists.newArrayList(table.allColumnsInSelectOrder());
                           return columnMetadata.stream().map(c -> new RawSelector(forQuoted(c.name.toString(), selectable.getTableAlias()), null));
                       }
                       else
                       {
                           return Stream.of(raw);
                       }
                   })
                   .map(s -> s.prepare(table))
                   .collect(Collectors.toList());
    }

    private Selectable prepare(TableMetadata table)
    {
        Selectable s = selectable.prepare(table);
        return alias != null ? new AliasedSelectable(s, alias) : s;
    }
}
