package org.atlasapi.media;

import org.atlasapi.media.content.EsContentIndexer;
import org.atlasapi.media.content.EsContentSearcher;
import org.atlasapi.media.content.schedule.EsScheduleIndex;
import org.atlasapi.media.topic.EsPopularTopicIndex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service.State;
import com.metabroadcast.common.time.SystemClock;

public class ElasticSearchContentIndexModule {

    private final EsContentIndexer contentIndexer;
    private final EsScheduleIndex scheduleIndex;
    private final EsPopularTopicIndex topicSearcher;
    private final EsContentSearcher contentSearcher;

    public ElasticSearchContentIndexModule(String seeds, long requestTimeout) {
        Node index = NodeBuilder.nodeBuilder().client(true).
                clusterName(EsSchema.CLUSTER_NAME).
                settings(ImmutableSettings.settingsBuilder().put("discovery.zen.ping.unicast.hosts", seeds)).
                build().start();
        this.contentIndexer = new EsContentIndexer(index, new SystemClock(), requestTimeout);
        this.scheduleIndex = new EsScheduleIndex(index, new SystemClock());
        this.topicSearcher = new EsPopularTopicIndex(index);
        this.contentSearcher = new EsContentSearcher(index);
    }

    public void init() {
        Futures.addCallback(contentIndexer.start(), new FutureCallback<State>() {

            private final Logger log = LoggerFactory.getLogger(ElasticSearchContentIndexModule.class);

            @Override
            public void onSuccess(State result) {
                log.info("Started index module");
            }

            @Override
            public void onFailure(Throwable t) {
                log.info("Failed to start index module:", t);
            }
        });
    }

    public EsContentIndexer contentIndexer() {
        return contentIndexer;
    }

    public EsScheduleIndex scheduleIndex() {
        return scheduleIndex;
    }

    public EsPopularTopicIndex topicSearcher() {
        return topicSearcher;
    }
    
    public EsContentSearcher contentSearcher() {
        return contentSearcher;
    }
}
