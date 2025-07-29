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

package org.apache.doris.datasource.setats.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.thrift.TExprOpcode;

import com.tencent.oceanus.predicate.Predicate;
import com.tencent.oceanus.predicate.PredicateBuilder;
import com.tencent.oceanus.shaded.org.apache.flink.table.data.binary.BinaryStringData;
import com.tencent.oceanus.shaded.org.apache.flink.table.types.logical.LogicalType;
import com.tencent.oceanus.shaded.org.apache.flink.table.types.logical.RowType;
import com.tencent.oceanus.shaded.org.apache.flink.table.types.logical.RowType.RowField;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class SetatsPredicateConverter {
    private final PredicateBuilder builder;
    private final List<String> fieldNames;
    private final List<LogicalType> setatsFieldTypes;

    public SetatsPredicateConverter(RowType rowType) {
        this.builder = new PredicateBuilder(rowType);
        this.fieldNames = rowType.getFields().stream().map(RowField::getName).collect(Collectors.toList());
        this.setatsFieldTypes = rowType.getFields().stream().map(RowField::getType).collect(Collectors.toList());
    }

    public Predicate convert(List<Expr> conjuncts) {
        List<Predicate> list = new ArrayList<>(conjuncts.size());
        for (Expr conjunct : conjuncts) {
            Predicate predicate = convert(conjunct);
            if (predicate != null) {
                list.add(predicate);
            }
        }
        return PredicateBuilder.andNullable(list);
    }

    public Predicate convert(Expr expr) {
        if (expr == null) {
            return null;
        }

        if (expr instanceof BoolLiteral) {
            // there is no alwaysTrue/alwaysFalse in setats's Predicate
            return null;
        }

        if (expr instanceof CompoundPredicate) {
            return compoundExpr((CompoundPredicate) expr);
        }

        if (expr instanceof BinaryPredicate) {
            return binaryExprDesc(expr);
        }

        if (expr instanceof InPredicate) {
            return inPredicate((InPredicate) expr);
        }

        return null;
    }

    private Predicate compoundExpr(CompoundPredicate compoundPredicate) {
        Predicate left = convert(compoundPredicate.getChild(0));
        Predicate right = convert(compoundPredicate.getChild(1));
        switch (compoundPredicate.getOp()) {
            case AND: {
                if (left != null && right != null) {
                    return PredicateBuilder.and(left, right);
                }
                return null;
            }
            case OR: {
                if (left != null && right != null) {
                    return PredicateBuilder.or(left, right);
                }
                return null;
            }
            case NOT:
                return left.negate().orElse(null);
            default:
                return null;
        }
    }

    private Predicate binaryExprDesc(Expr expr) {
        TExprOpcode opcode = expr.getOpcode();
        SlotRef slotRef = convertDorisExprToSlotRef(expr.getChild(0));
        LiteralExpr literalExpr = null;
        if (slotRef == null && expr.getChild(0).isLiteral()) {
            literalExpr = (LiteralExpr) expr.getChild(0);
            slotRef = convertDorisExprToSlotRef(expr.getChild(1));
        } else if (expr.getChild(1).isLiteral()) {
            literalExpr = (LiteralExpr) expr.getChild(1);
        }
        if (slotRef == null || literalExpr == null) {
            return null;
        }
        String colName = slotRef.getColumnName();
        int idx = fieldNames.indexOf(colName);
        LogicalType logicalType = setatsFieldTypes.get(idx);
        Object value = logicalType.accept(new SetatsValueConverter(literalExpr));
        if (value == null) {
            return null;
        }
        switch (opcode) {
            case EQ:
                return builder.equal(idx, value);
            case EQ_FOR_NULL:
                return builder.isNull(idx);
            case NE:
                return builder.notEqual(idx, value);
            case GE:
                return builder.greaterOrEqual(idx, value);
            case GT:
                return builder.greaterThan(idx, value);
            case LE:
                return builder.lessOrEqual(idx, value);
            case LT:
                return builder.lessThan(idx, value);
            case INVALID_OPCODE:
                if (expr instanceof FunctionCallExpr) {
                    String name = expr.getExprName().toLowerCase();
                    String s = value.toString();
                    if (name.equals("like") && !s.startsWith("%") && s.endsWith("%")) {
                        return builder.startsWith(idx, BinaryStringData.fromString(s.substring(0, s.length() - 1)));
                    }
                } else if (expr instanceof IsNullPredicate) {
                    if (((IsNullPredicate) expr).isNotNull()) {
                        return builder.isNotNull(idx);
                    } else {
                        return builder.isNull(idx);
                    }
                }
                return null;
            default:
                return null;
        }

    }

    private Predicate inPredicate(InPredicate inExpr) {
        if (inExpr.contains(Subquery.class)) {
            return null;
        }
        SlotRef slotRef = convertDorisExprToSlotRef(inExpr.getChild(0));
        if (slotRef == null) {
            return null;
        }
        String colName = slotRef.getColumnName();
        int idx = fieldNames.indexOf(colName);
        LogicalType logicalType = setatsFieldTypes.get(idx);
        List<Object> valueList = new ArrayList<>();
        for (int i = 1; i < inExpr.getChildren().size(); ++i) {
            if (!(inExpr.getChild(i) instanceof LiteralExpr)) {
                return null;
            }
            LiteralExpr literalExpr = (LiteralExpr) inExpr.getChild(i);
            Object value = logicalType.accept(new SetatsValueConverter(literalExpr));
            valueList.add(value);
        }
        if (inExpr.isNotIn()) {
            return builder.notIn(idx, valueList);
        } else {
            return builder.in(idx, valueList);
        }
    }


    public static SlotRef convertDorisExprToSlotRef(Expr expr) {
        SlotRef slotRef = null;
        if (expr instanceof SlotRef) {
            slotRef = (SlotRef) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof SlotRef) {
                slotRef = (SlotRef) expr.getChild(0);
            }
        }
        return slotRef;
    }

    public LiteralExpr convertDorisExprToLiteralExpr(Expr expr) {
        LiteralExpr literalExpr = null;
        if (expr instanceof LiteralExpr) {
            literalExpr = (LiteralExpr) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof LiteralExpr) {
                literalExpr = (LiteralExpr) expr.getChild(0);
            }
        }
        return literalExpr;
    }
}
