// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Eliminate impossible grouping sets according to predicates above aggregate.
 * For example:
 * <pre>
 * Logical plan tree:
 *             filter (a = 1)
 *                |
 *      aggregate with source repeat (grouping sets ((a, b), ()))
 * transformed to:
 *             filter (a = 1)
 *                |
 *      aggregate with source repeat (grouping sets ((a, b)))
 * </pre>
 * Note:
 * For one grouping set, expressions not in that grouping set will be filled with null in repeat output.
 * If substituting those expressions with null makes any conjunct evaluate to false/null,
 * that grouping set can be pruned safely.
 */
public class EliminateGroupingSets implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalFilter(logicalAggregate(logicalProject(logicalRepeat(any())))).when(
                                filter -> filter.child().getSourceRepeat().isPresent())
                        .thenApply(ctx -> pruneSourceRepeatGroupingSetsByFilter(ctx.root, ctx.cascadesContext))
                        .toRule(RuleType.ELIMINATE_GROUPING_SETS)
        );
    }

    /**
     * Prune grouping sets in aggregate.sourceRepeat according to filter conjuncts.
     * Keep sourceRepeat and project child repeat consistent after pruning.
     */
    private Plan pruneSourceRepeatGroupingSetsByFilter(
            LogicalFilter<LogicalAggregate<LogicalProject<LogicalRepeat<Plan>>>> filter,
            CascadesContext cascadesContext) {
        LogicalAggregate<LogicalProject<LogicalRepeat<Plan>>> aggregate = filter.child();
        LogicalProject<LogicalRepeat<Plan>> project = aggregate.child();
        LogicalRepeat<? extends Plan> sourceRepeat = aggregate.getSourceRepeat().get();
        LogicalRepeat<? extends Plan> childRepeat = project.child();
        if (!sourceRepeat.equals(childRepeat)) {
            return filter;
        }
        List<List<Expression>> groupingSets = childRepeat.getGroupingSets();

        if (groupingSets == null) {
            return filter;
        }

        Set<Expression> groupingSetExpressions = childRepeat.getGroupingSetExpressions();
        List<List<Expression>> pruned = new ArrayList<>(groupingSets.size());
        for (List<Expression> gs : groupingSets) {
            Set<Expression> nullGroupingExpressions = Sets.difference(groupingSetExpressions, new HashSet<>(gs));
            if (nullGroupingExpressions.isEmpty()) {
                pruned.add(gs);
                continue;
            }
            if (retainGroupingSet(nullGroupingExpressions, filter.getConjuncts(), cascadesContext)) {
                pruned.add(gs);
            }
        }

        if (pruned.isEmpty()) {
            return new LogicalEmptyRelation(ConnectContext.get().getStatementContext().getNextRelationId(),
                    filter.getOutput());
        }
        if (pruned.size() == groupingSets.size()) {
            return filter;
        }

        LogicalRepeat<? extends Plan> newChildRepeat = childRepeat.withGroupSets(pruned);
        return filter.withChildren(
                aggregate.withSourceRepeat(newChildRepeat).withChildren(project.withChildren(newChildRepeat)));
    }

    /**
     * Check whether one grouping set should be retained.
     *
     * Replace expressions that become null under this grouping set with null literals,
     * then constant-fold each predicate. If any predicate becomes FALSE or NULL,
     * this grouping set can never pass filter and should be removed.
     */
    private boolean retainGroupingSet(Set<Expression> nullExpressions, Set<Expression> predicates,
            CascadesContext cascadesContext) {
        Map<Expression, Expression> replaceMap = new HashMap<>();
        for (Expression p : nullExpressions) {
            Literal nullLiteral = new NullLiteral(p.getDataType());
            replaceMap.put(p, nullLiteral);
        }
        for (Expression predicate : predicates) {
            Expression evalExpr = FoldConstantRule.evaluate(
                    ExpressionUtils.replace(predicate, replaceMap),
                    new ExpressionRewriteContext(cascadesContext)
            );
            if (evalExpr.isNullLiteral() || BooleanLiteral.FALSE.equals(evalExpr)) {
                return false;
            }
        }

        return true;
    }
}
