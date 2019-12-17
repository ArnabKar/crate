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

package io.crate.analyze.relations;

import io.crate.action.sql.SessionContext;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.ParameterExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class StatementAnalysisContext {

    private final Operation currentOperation;
    private final CoordinatorTxnCtx coordinatorTxnCtx;
    private final List<Reference> parentOutputColumns;
    private final Function<ParameterExpression, Symbol> convertParamFunction;
    private final List<RelationAnalysisContext> lastRelationContextQueue = new ArrayList<>();

    public StatementAnalysisContext(Function<ParameterExpression, Symbol> convertParamFunction,
                                    Operation currentOperation,
                                    CoordinatorTxnCtx coordinatorTxnCtx,
                                    List<Reference> parentOutputColumns) {
        this.convertParamFunction = convertParamFunction;
        this.currentOperation = currentOperation;
        this.coordinatorTxnCtx = coordinatorTxnCtx;
        this.parentOutputColumns = parentOutputColumns;
    }

    public StatementAnalysisContext(Function<ParameterExpression, Symbol> convertParamFunction,
                                    Operation currentOperation,
                                    CoordinatorTxnCtx coordinatorTxnCtx) {
        this(convertParamFunction, currentOperation, coordinatorTxnCtx, List.of());
    }

    public CoordinatorTxnCtx transactionContext() {
        return coordinatorTxnCtx;
    }

    Operation currentOperation() {
        return currentOperation;
    }

    public RelationAnalysisContext startRelation() {
        return startRelation(false);
    }

    RelationAnalysisContext startRelation(boolean aliasedRelation) {
        ParentRelations parentRelations;
        if (lastRelationContextQueue.isEmpty()) {
            parentRelations = ParentRelations.NO_PARENTS;
        } else {
            RelationAnalysisContext parentCtx = lastRelationContextQueue.get(lastRelationContextQueue.size() - 1);
            parentRelations = parentCtx.parentSources().newLevel(parentCtx.sources());
        }
        RelationAnalysisContext currentRelationContext =
            new RelationAnalysisContext(aliasedRelation, parentRelations);
        lastRelationContextQueue.add(currentRelationContext);
        return currentRelationContext;
    }

    public void endRelation() {
        if (lastRelationContextQueue.size() > 0) {
            lastRelationContextQueue.remove(lastRelationContextQueue.size() - 1);
        }
    }

    RelationAnalysisContext currentRelationContext() {
        assert lastRelationContextQueue.size() > 0 : "relation context must be created using startRelation() first";
        return lastRelationContextQueue.get(lastRelationContextQueue.size() - 1);
    }

    public SessionContext sessionContext() {
        return coordinatorTxnCtx.sessionContext();
    }

    public Function<ParameterExpression,Symbol> convertParamFunction() {
        return convertParamFunction;
    }

    public List<Reference> parentOutputColumns() {
        return parentOutputColumns;
    }
}
