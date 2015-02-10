/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.node.dml;

import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.uid.Versions;

import java.util.ArrayList;
import java.util.List;

public class UpsertByIdNode2 extends DMLPlanNode {

    /**
     * A single update item.
     */
    public static class Item {

        private final String index;
        private final String id;
        private final String routing;
        private long version = Versions.MATCH_ANY;
        private Object[] row;

        Item(String index,
                    String id,
                    String routing,
                    Object[] row,
                    @Nullable Long version) {
            this.index = index;
            this.id = id;
            this.routing = routing;
            if (version != null) {
                this.version = version;
            }
            this.row = row;
        }

        public String index() {
            return index;
        }

        public String id() {
            return id;
        }

        public String routing() {
            return routing;
        }

        public long version() {
            return version;
        }

        public Object[] row() {
            return row;
        }
    }


    private final boolean partitionedTable;
    private final boolean bulkRequest;
    private final List<Item> items;
    private final DataType[] dataTypes;
    @Nullable
    private final String[] updateColumns;
    @Nullable
    private final String[] insertColumns;
    @Nullable
    private final Symbol[] updateAssignments;
    @Nullable
    private final Symbol[] insertAssignments;

    public UpsertByIdNode2(boolean partitionedTable,
                           boolean bulkRequest,
                           DataType[] dataTypes,
                           @Nullable String[] updateColumns,
                           @Nullable String[] insertColumns,
                           @Nullable Symbol[] updateAssignments,
                           @Nullable Symbol[] insertAssignments) {
        this.partitionedTable = partitionedTable;
        this.bulkRequest = bulkRequest;
        this.dataTypes = dataTypes;
        this.updateColumns = updateColumns;
        this.insertColumns = insertColumns;
        this.updateAssignments = updateAssignments;
        this.insertAssignments = insertAssignments;
        this.items = new ArrayList<>();
    }

    public DataType[] dataTypes() {
        return dataTypes;
    }

    @Nullable
    public String[] updateColumns() {
        return updateColumns;
    }

    @Nullable
    public String[] insertColumns() {
        return insertColumns;
    }

    @Nullable
    public Symbol[] updateAssignments() {
        return updateAssignments;
    }

    @Nullable
    public Symbol[] insertAssignments() {
        return insertAssignments;
    }

    public boolean isPartitionedTable() {
        return partitionedTable;
    }

    public boolean isBulkRequest() {
        return bulkRequest;
    }

    public void add(String index,
                    String id,
                    String routing,
                    Object[] row,
                    @Nullable Long version) {
        items.add(new Item(index, id, routing, row, version));
    }

    public List<Item> items() {
        return items;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitUpsertByIdNode2(this, context);
    }
}
