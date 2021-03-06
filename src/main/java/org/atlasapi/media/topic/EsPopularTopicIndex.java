package org.atlasapi.media.topic;

import org.atlasapi.media.EsSchema;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.EsBroadcast;
import org.atlasapi.media.content.EsContent;
import org.atlasapi.media.content.EsTopicMapping;
import org.atlasapi.media.util.FutureSettingActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.query.Selection;

public class EsPopularTopicIndex implements PopularTopicIndex {

    private final Node index;

    public EsPopularTopicIndex(Node index) {
        this.index = index;
    }
    
    @Override
    public ListenableFuture<FluentIterable<Id>> popularTopics(Interval interval, final Selection selection) {
        SettableFuture<SearchResponse> response = SettableFuture.create();
        prepareQuery(interval, selection)
            .execute(FutureSettingActionListener.setting(response));
        return Futures.transform(response, new Function<SearchResponse, FluentIterable<Id>>() {
            @Override
            public FluentIterable<Id> apply(SearchResponse input) {
                Facets facets = input.getFacets();
                TermsFacet terms = facets.facet(TermsFacet.class, EsContent.TOPICS);
                return FluentIterable.from(terms).skip(selection.getOffset()).transform(new Function<Entry, Id>() {
                    @Override
                    public Id apply(Entry input) {
                        return Id.valueOf(input.getTerm());
                    }
                });
            }
        });
    }

    private SearchRequestBuilder prepareQuery(Interval interval, Selection selection) {
        return index.client()
            .prepareSearch(EsSchema.INDEX_NAME)
            .setQuery(QueryBuilders.nestedQuery(EsContent.BROADCASTS, 
                QueryBuilders.rangeQuery(EsBroadcast.TRANSMISSION_TIME)
                    .from(interval.getStart())
                    .to(interval.getEnd())
            ))
            .addFacet(FacetBuilders.termsFacet(EsContent.TOPICS)
                .field(EsContent.TOPICS + "." + EsTopicMapping.ID)
                .size(selection.getOffset() + selection.getLimit())
            );
    }
    
}
