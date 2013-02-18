package org.atlasapi.media.util;

import java.util.Date;
import java.util.List;
import java.util.Stack;

import org.atlasapi.content.criteria.AttributeQuery;
import org.atlasapi.content.criteria.BooleanAttributeQuery;
import org.atlasapi.content.criteria.DateTimeAttributeQuery;
import org.atlasapi.content.criteria.EnumAttributeQuery;
import org.atlasapi.content.criteria.IdAttributeQuery;
import org.atlasapi.content.criteria.IntegerAttributeQuery;
import org.atlasapi.content.criteria.MatchesNothing;
import org.atlasapi.content.criteria.AttributeQuerySet;
import org.atlasapi.content.criteria.QueryNode;
import org.atlasapi.content.criteria.QueryNode.IntermediateNode;
import org.atlasapi.content.criteria.QueryNode.TerminalNode;
import org.atlasapi.content.criteria.QueryNodeVisitor;
import org.atlasapi.content.criteria.QueryVisitor;
import org.atlasapi.content.criteria.StringAttributeQuery;
import org.atlasapi.content.criteria.operator.BooleanOperatorVisitor;
import org.atlasapi.content.criteria.operator.DateTimeOperatorVisitor;
import org.atlasapi.content.criteria.operator.EnumOperatorVisitor;
import org.atlasapi.content.criteria.operator.IntegerOperatorVisitor;
import org.atlasapi.content.criteria.operator.Operators.After;
import org.atlasapi.content.criteria.operator.Operators.Before;
import org.atlasapi.content.criteria.operator.Operators.Beginning;
import org.atlasapi.content.criteria.operator.Operators.Equals;
import org.atlasapi.content.criteria.operator.Operators.GreaterThan;
import org.atlasapi.content.criteria.operator.Operators.LessThan;
import org.atlasapi.content.criteria.operator.StringOperatorVisitor;
import org.atlasapi.media.common.Id;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

public class EsQueryBuilder {

    private static final Joiner PATH_JOINER = Joiner.on(".");

    public QueryBuilder buildQuery(AttributeQuerySet operands) {
        return operands.accept(new QueryNodeVisitor<QueryBuilder>() {

            Stack<String> traversed = new Stack<String>();
            
            @Override
            public QueryBuilder visit(IntermediateNode node) {
                BoolQueryBuilder conjuncts = QueryBuilders.boolQuery();
                traversed.addAll(node.pathSegments());
                for (QueryNode desc : node.getDescendants()) {
                    conjuncts.must(desc.accept(this));
                }
                for(int i = 0; i < node.pathSegments().size(); i++) traversed.pop();
                return node.pathSegments().isEmpty() ? conjuncts 
                                                     : nest(node, conjuncts);
            }

            private NestedQueryBuilder nest(IntermediateNode node, BoolQueryBuilder conjuncts) {
                return QueryBuilders.nestedQuery(
                    nestPath(node), conjuncts
                );
            }

            private String nestPath(IntermediateNode node) {
                if (traversed.isEmpty()) {
                    return PATH_JOINER.join(node.pathSegments());
                }
                StringBuilder builder = new StringBuilder();
                builder = PATH_JOINER.appendTo(builder, traversed).append('.');
                return PATH_JOINER.appendTo(builder, node.pathSegments()).toString();
            }

            @Override
            public QueryBuilder visit(TerminalNode node) {
                AttributeQuery<?> query = node.getQuery();
                QueryBuilder builder = queryForTerminal(node);
                if (node.pathSegments().size() > 1) {
                    builder = QueryBuilders.nestedQuery(query.getAttribute().getPathPrefix(), builder);
                }
                return builder;
            }
        });
    }

    private QueryBuilder queryForTerminal(TerminalNode node) {
        return node.getQuery().accept(new QueryVisitor<QueryBuilder>() {

            @Override
            public QueryBuilder visit(final IntegerAttributeQuery query) {
                final String name = query.getAttributeName();
                final List<Integer> values = query.getValue();
                return query.accept(new IntegerOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        Object[] vals = values.toArray(new Object[values.size()]);
                        return QueryBuilders.termsQuery(name, vals);
                    }

                    @Override
                    public QueryBuilder visit(LessThan lessThan) {
                        return QueryBuilders.rangeQuery(name)
                            .lt(Ordering.natural().max(values));
                    }

                    @Override
                    public QueryBuilder visit(GreaterThan greaterThan) {
                        return QueryBuilders.rangeQuery(name)
                            .gt(Ordering.natural().min(values));
                    }
                });
            }

            @Override
            public QueryBuilder visit(StringAttributeQuery query) {
                final List<String> values = query.getValue();
                final String name = query.getAttributeName();
                return query.accept(new StringOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        Object[] vals = values.toArray(new Object[values.size()]);
                        return QueryBuilders.termsQuery(name, vals);
                    }

                    @Override
                    public QueryBuilder visit(Beginning beginning) {
                        return QueryBuilders.prefixQuery(name, values.get(0));
                    }
                    
                });
            }

            @Override
            public QueryBuilder visit(BooleanAttributeQuery query) {
                final String name = query.getAttributeName();
                final boolean value = query.getValue().get(0);
                return query.accept(new BooleanOperatorVisitor<QueryBuilder>() {
                    @Override
                    public QueryBuilder visit(Equals equals) {
                        return QueryBuilders.termQuery(name, value);
                    }
                });
            }

            @Override
            public QueryBuilder visit(EnumAttributeQuery<?> query) {
                final String name = query.getAttributeName();
                final List<?> values = query.getValue();
                return query.accept(new EnumOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        Object[] vals = values.toArray(new Object[values.size()]);
                        return QueryBuilders.termsQuery(name, vals);
                    }
                });
            }

            @Override
            public QueryBuilder visit(DateTimeAttributeQuery query) {
                final String name = query.getAttributeName();
                final List<Date> values = toDates(query.getValue());
                return query.accept(new DateTimeOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Before before) {
                        return QueryBuilders.rangeQuery(name)
                            .lt(Ordering.natural().max(values));
                    }

                    @Override
                    public QueryBuilder visit(After after) {
                        return QueryBuilders.rangeQuery(name)
                            .gt(Ordering.natural().min(values));
                    }

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        return QueryBuilders.termsQuery(name, values);
                    }
                });
            }

            private List<Date> toDates(List<DateTime> value) {
                return Lists.transform(value, new Function<DateTime, Date>() {
                    @Override
                    public Date apply(DateTime input) {
                        return input.toDate();
                    }
                });
            }

            @Override
            public QueryBuilder visit(MatchesNothing noOp) {
                throw new IllegalArgumentException();
            }

            @Override
            public QueryBuilder visit(IdAttributeQuery query) {
                final String name = query.getAttributeName();
                final List<Long> value = Lists.transform(query.getValue(), Id.toLongValue());
                return query.accept(new IntegerOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        Object[] values = value.toArray(new Object[value.size()]);
                        return QueryBuilders.termsQuery(name, values);
                    }

                    @Override
                    public QueryBuilder visit(LessThan lessThan) {
                        return QueryBuilders.rangeQuery(name)
                            .lt(Ordering.natural().max(value));
                    }

                    @Override
                    public QueryBuilder visit(GreaterThan greaterThan) {
                        return QueryBuilders.rangeQuery(name)
                            .gt(Ordering.natural().min(value));
                    }
                });
            }
        });
    }
    
}
