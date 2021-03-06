package org.atlasapi.media.content;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.media.topic.EsTopic.ID;
import static org.atlasapi.media.topic.EsTopic.SOURCE;

import org.atlasapi.content.criteria.AttributeQuerySet;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.util.EsQueryBuilder;
import org.atlasapi.media.util.FiltersBuilder;
import org.atlasapi.media.util.FutureSettingActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.query.Selection;


public class EsContentIndex implements ContentIndex {

    private static final int DEFAULT_LIMIT = 50;

    private final Node esClient;
    private final String indexName;
    
    private final EsQueryBuilder builder = new EsQueryBuilder();

    public EsContentIndex(Node esClient, String indexName) {
        this.esClient = checkNotNull(esClient);
        this.indexName = checkNotNull(indexName);
    }
    
    @Override
    public ListenableFuture<FluentIterable<Id>> query(AttributeQuerySet query, 
        Iterable<Publisher> publishers, Selection selection) {
        SettableFuture<SearchResponse> response = SettableFuture.create();
        esClient.client()  
            .prepareSearch(indexName)
            .setTypes(EsContent.CHILD_TYPE, EsContent.TOP_LEVEL_TYPE, EsContent.TOP_ITEM_TYPE)
            .setQuery(builder.buildQuery(query))
            .addField(ID)
            .setFilter(FiltersBuilder.buildForPublishers(SOURCE, publishers))
            .setFrom(selection.getOffset())
            .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT))
            .execute(FutureSettingActionListener.setting(response));
        
        return Futures.transform(response, new Function<SearchResponse, FluentIterable<Id>>() {
            @Override
            public FluentIterable<Id> apply(SearchResponse input) {
                /*
                 * TODO: if 
                 *  selection.offset + selection.limit < totalHits
                 * then we have more: return for use with response. 
                 */
                return FluentIterable.from(input.getHits()).transform(new Function<SearchHit, Id>() {
                    @Override
                    public Id apply(SearchHit hit) {
                        Long id = hit.field(ID).<Number>value().longValue();
                        return Id.valueOf(id);
                    }
                });
            }
        });
    }
}
