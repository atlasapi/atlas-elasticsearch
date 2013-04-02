package org.atlasapi.media;

import java.util.concurrent.TimeUnit;

import org.atlasapi.media.content.EsContentIndex;
import org.atlasapi.media.content.EsContentIndexer;
import org.atlasapi.media.content.EsContentSearcher;
import org.atlasapi.media.content.schedule.EsScheduleIndex;
import org.atlasapi.media.topic.EsPopularTopicIndex;
import org.atlasapi.media.topic.EsTopicIndex;
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

    private final Logger log = LoggerFactory.getLogger(ElasticSearchContentIndexModule.class);

    private final EsContentIndexer contentIndexer;
    private final EsContentIndex contentIndex;
    private final EsScheduleIndex scheduleIndex;
    private final EsTopicIndex topicIndex;
    private final EsPopularTopicIndex popularTopicsIndex;
    private final EsContentSearcher contentSearcher;

    public ElasticSearchContentIndexModule(String seeds, String clusterName, long requestTimeout) {
        Node client = NodeBuilder.nodeBuilder().client(true).
                clusterName(clusterName).
                settings(ImmutableSettings.settingsBuilder().put("discovery.zen.ping.unicast.hosts", seeds)).
                build().start();
        this.contentIndexer = new EsContentIndexer(client, EsSchema.CONTENT_INDEX, new SystemClock(), requestTimeout);
        this.contentIndex = new EsContentIndex(client, EsSchema.CONTENT_INDEX);
        this.scheduleIndex = new EsScheduleIndex(client, new SystemClock());
        this.popularTopicsIndex = new EsPopularTopicIndex(client);
        this.topicIndex = new EsTopicIndex(client, EsSchema.TOPICS_INDEX, 60, TimeUnit.SECONDS);
        this.contentSearcher = new EsContentSearcher(client);
    }

    public void init() {
        //Investigate service manager?
        Futures.addCallback(contentIndexer.start(), new FutureCallback<State>() {

            @Override
            public void onSuccess(State result) {
                log.info("Started index module");
            }

            @Override
            public void onFailure(Throwable t) {
                log.info("Failed to start index module:", t);
            }
        });
        Futures.addCallback(topicIndex.start(), new FutureCallback<State>() {
            
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
    
    public EsContentIndex contentIndex() {
        return contentIndex;
    }

    public EsScheduleIndex scheduleIndex() {
        return scheduleIndex;
    }
    
    public EsTopicIndex topicIndex() {
        return topicIndex;
    }

    public EsPopularTopicIndex topicSearcher() {
        return popularTopicsIndex;
    }
    
    public EsContentSearcher contentSearcher() {
        return contentSearcher;
    }
}
