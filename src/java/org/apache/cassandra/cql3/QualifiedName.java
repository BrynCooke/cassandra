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
package org.apache.cassandra.cql3;

import java.util.Locale;
import java.util.Objects;

/**
 * Class for the names of the keyspace-prefixed elements (e.g. table, index, view names)
 */
public class QualifiedName
{
    /**
     * The keyspace name as stored internally.
     */
    private String keyspace;
    private String name;
    private ColumnIdentifier alias;

    public QualifiedName()
    {
    }

    public QualifiedName(String keyspace, String name)
    {
        this.keyspace = keyspace;
        this.name = name;
    }

    /**
     * Sets the keyspace.
     *
     * @param ks the keyspace name
     * @param keepCase <code>true</code> if the case must be kept, <code>false</code> otherwise.
     */
    public final void setKeyspace(String ks, boolean keepCase)
    {
        keyspace = toInternalName(ks, keepCase);
    }

    /**
     * Sets the alias.
     *
     * @param alias the alias
     */
    public final void setAlias(ColumnIdentifier alias)
    {
        this.alias = alias;
    }


    /**
     * Checks if the keyspace is specified.
     * @return <code>true</code> if the keyspace is specified, <code>false</code> otherwise.
     */
    public final boolean hasKeyspace()
    {
        return keyspace != null;
    }

    public final String getKeyspace()
    {
        return keyspace;
    }

    /**
     * Checks if the alias is specified.
     * @return <code>true</code> if the alias is specified, <code>false</code> otherwise.
     */
    public final boolean hasAlias()
    {
        return alias != null;
    }

    public final ColumnIdentifier getAlias()
    {
        return alias;
    }

    public void setName(String cf, boolean keepCase)
    {
        name = toInternalName(cf, keepCase);
    }

    public String getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        if (hasKeyspace() || hasAlias())
        {
            StringBuilder builder = new StringBuilder();
            if (hasKeyspace())
            {
                builder.append(keyspace);
                builder.append(".");
            }
            builder.append(name);
            if (hasAlias())
            {
                builder.append("(");
                builder.append(alias);
                builder.append(")");
            }
            return builder.toString();
        }
        return name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, name, alias);
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof QualifiedName))
            return false;

        QualifiedName qn = (QualifiedName) o;
        return Objects.equals(keyspace, qn.keyspace) && name.equals(qn.name) && Objects.equals(alias, qn.alias);
    }

    /**
     * Converts the specified name into the name used internally.
     *
     * @param name the name
     * @param keepCase <code>true</code> if the case must be kept, <code>false</code> otherwise.
     * @return the name used internally.
     */
    private static String toInternalName(String name, boolean keepCase)
    {
        return keepCase ? name : name.toLowerCase(Locale.US);
    }
}
