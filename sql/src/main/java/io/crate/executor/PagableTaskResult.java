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

package io.crate.executor;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import java.util.NoSuchElementException;

/**
 * enhanced TaskResult that supports fetching the next page
 * asynchronously.
 */
public interface PagableTaskResult extends TaskResult {

    public static final PagableTaskResult EMPTY_PAGABLE_RESULT = new PagableTaskResult() {
        @Override
        public ListenableFuture<PagableTaskResult> fetch(PageInfo pageInfo) {
            return Futures.immediateFailedFuture(new NoSuchElementException());
        }

        @Override
        public Object[][] rows() {
            return TaskResult.EMPTY_ROWS;
        }

        @Nullable
        @Override
        public String errorMessage() {
            return null;
        }
    };

    /**
     * get the page identified by <code>pageInfo</code>.
     *
     * @param pageInfo identifying the page to fetch
     * @return a future holding the result of fetching the next page
     */
    ListenableFuture<PagableTaskResult> fetch(PageInfo pageInfo);
}
