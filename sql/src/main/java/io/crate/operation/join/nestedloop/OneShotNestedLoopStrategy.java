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

package io.crate.operation.join.nestedloop;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import io.crate.executor.Page;
import io.crate.executor.PageInfo;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.operation.projectors.Projector;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * nestedloop execution that produces all rows at once and returns them as
 * one single instance of {@linkplain io.crate.executor.QueryResult}.
 *
 * This is used, when NestedLoop is not paged.
 */
class OneShotNestedLoopStrategy implements NestedLoopStrategy {

    static class OneShotExecutor implements NestedLoopExecutor {
        private final JoinContext ctx;
        private final FutureCallback<Void> callback;

        private final Projector downstream;

        public OneShotExecutor(JoinContext ctx,
                                 Optional<PageInfo> globalPageInfo,
                                 Projector downstream,
                                 FutureCallback<Void> finalCallback) {
            this.ctx = ctx;
            this.callback = finalCallback;
            this.downstream = downstream;
        }

        @Override
        public void startExecution() {
            // assume taskresults with pages are already there
            Page outerPage = ctx.outerTaskResult.page();
            Outer:
            while (outerPage.size() > 0) {
                Page innerPage = ctx.innerTaskResult.page();
                try {
                    while (innerPage.size() > 0) {
                        ctx.outerPageIterator = outerPage.iterator();
                        ctx.innerPageIterator = innerPage.iterator();

                        // set outer row
                        if (!ctx.advanceOuterRow()) {
                            // outer page is empty
                            callback.onSuccess(null);
                        }

                        boolean wantMore = joinPages();
                        if (!wantMore) {
                            callback.onSuccess(null);
                            return;
                        }
                        innerPage = ctx.fetchInnerPage(ctx.advanceInnerPageInfo());
                    }
                    ctx.fetchInnerPage(ctx.resetInnerPageInfo()); // reset inner iterator
                    outerPage = ctx.fetchOuterPage(ctx.advanceOuterPageInfo());
                } catch (InterruptedException | ExecutionException e) {
                    callback.onFailure(e);
                    return;
                }
            }
            callback.onSuccess(null);
        }

        private boolean joinPages() {
            boolean wantMore = true;
            Object[] innerRow;

            Outer:
            do {
                while (ctx.innerPageIterator.hasNext()) {
                    innerRow = ctx.innerPageIterator.next();
                    wantMore = this.downstream.setNextRow(
                            ctx.combine(ctx.outerRow(), innerRow)
                    );
                    if (!wantMore) {
                        break Outer;
                    }
                }
                // reset inner iterator
                ctx.innerPageIterator = ctx.innerTaskResult.page().iterator();
            } while (ctx.advanceOuterRow());
            return wantMore;
        }

        @Override
        public void carryOnExecution() {
            // try to carry on where we left of
            boolean wantMore = joinPages();
            if (wantMore) {
                startExecution();
            } else {
                callback.onSuccess(null);
            }
        }
    }

    protected NestedLoopOperation nestedLoopOperation;
    protected NestedLoopExecutorService nestedLoopExecutorService;

    public OneShotNestedLoopStrategy(NestedLoopOperation nestedLoopOperation,
                                     NestedLoopExecutorService nestedLoopExecutorService) {
        this.nestedLoopOperation = nestedLoopOperation;
        this.nestedLoopExecutorService = nestedLoopExecutorService;
    }

    @Override
    public NestedLoopExecutor executor(JoinContext ctx, Optional<PageInfo> pageInfo, Projector downstream, FutureCallback<Void> callback) {
        return new OneShotExecutor(ctx, pageInfo, downstream, callback);
    }

    @Override
    public TaskResult emptyResult() {
        return TaskResult.EMPTY_RESULT;
    }

    @Override
    public int rowsToProduce(Optional<PageInfo> pageInfo) {
        return nestedLoopOperation.limit() + nestedLoopOperation.offset();
    }

    @Override
    public void onFirstJoin(JoinContext joinContext) {
        // we can close the context as we produced ALL results in one batch
        try {
            joinContext.close();
        } catch (IOException e) {
            nestedLoopOperation.logger.error("error closing joinContext after {} NestedLoop execution", e, name());
        }
    }

    @Override
    public TaskResult produceFirstResult(Object[][] rows, Optional<PageInfo> pageInfo, JoinContext joinContext) {
        return new QueryResult(rows);
    }

    @Override
    public String name() {
        return "one shot";
    }
}
