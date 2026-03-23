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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class EliminateGroupingSetsTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = new LogicalOlapScan(
            StatementScopeIdGenerator.newRelationId(), PlanConstructor.student, ImmutableList.of(""));

    @Test
    void testBasicPruneGroupingSets() {
        Slot id = scan.getOutput().get(0);
        Slot gender = scan.getOutput().get(1);
        Slot name = scan.getOutput().get(2);

        LogicalRepeat<?> repeat = new LogicalRepeat<>(
                ImmutableList.of(ImmutableList.of(id, gender), ImmutableList.of()),
                ImmutableList.of(id, gender, name),
                scan);

        LogicalPlan plan = new LogicalPlanBuilder(repeat)
                .projectAll()
                .aggGroupUsingIndexAndSourceRepeat(
                        ImmutableList.of(0, 1),
                        ImmutableList.of(id, gender),
                        Optional.of(repeat))
                .filter(new EqualTo(id, Literal.of(1)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateGroupingSets())
                .matches(logicalFilter(logicalAggregate().when(agg ->
                        agg.getSourceRepeat().isPresent()
                                && agg.getSourceRepeat().get().getGroupingSets().size() == 1
                                && agg.getSourceRepeat().get().getGroupingSets().get(0).size() == 2)));
    }

    @Test
    void testPruneGroupingSetsWithFunctionInCube() {
        Slot name = scan.getOutput().get(2);
        Alias concatNameX = new Alias(new Concat(name, new StringLiteral("x")), "namex");

        LogicalRepeat<?> repeat = new LogicalRepeat<>(
                ImmutableList.of(
                        ImmutableList.of(concatNameX.toSlot(), name),
                        ImmutableList.of(concatNameX.toSlot()),
                        ImmutableList.of(name),
                        ImmutableList.of()),
                ImmutableList.of(concatNameX, name),
                scan);

        LogicalPlan plan = new LogicalPlanBuilder(repeat)
                .projectAll()
                .aggGroupUsingIndexAndSourceRepeat(
                        ImmutableList.of(0, 1),
                        ImmutableList.of((NamedExpression) concatNameX, name),
                        Optional.of(repeat))
                .filter(new EqualTo(concatNameX.toSlot(), new StringLiteral("abcx")))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateGroupingSets())
                .matches(logicalFilter(logicalAggregate().when(agg ->
                        agg.getSourceRepeat().isPresent()
                                && agg.getSourceRepeat().get().getGroupingSets().size() == 2
                                && agg.getSourceRepeat().get().getGroupingSets().stream().allMatch(gs ->
                                gs.stream().anyMatch(e -> e.toSql().toLowerCase().contains("namex"))))));
    }

    @Test
    void testAllGroupingSetsPrunedToEmptyRelation() {
        Slot name = scan.getOutput().get(2);
        Alias concatNameX = new Alias(new Concat(name, new StringLiteral("x")), "namex");

        LogicalRepeat<?> repeat = new LogicalRepeat<>(
                ImmutableList.of(
                        ImmutableList.of(concatNameX.toSlot()),
                        ImmutableList.of(name)),
                ImmutableList.of(concatNameX, name),
                scan);

        Expression filterPredicate = ExpressionUtils.and(
                new EqualTo(concatNameX.toSlot(), new StringLiteral("abcx")),
                new EqualTo(name, new StringLiteral("uniswap"))
        );

        LogicalPlan plan = new LogicalPlanBuilder(repeat)
                .projectAll()
                .aggGroupUsingIndexAndSourceRepeat(
                        ImmutableList.of(0, 1),
                        ImmutableList.of(concatNameX, name),
                        Optional.of(repeat))
                .filter(filterPredicate)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateGroupingSets())
                .matches(logicalEmptyRelation());
    }
}
