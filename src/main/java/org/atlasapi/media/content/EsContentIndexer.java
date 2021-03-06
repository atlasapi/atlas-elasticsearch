package org.atlasapi.media.content;

import static org.atlasapi.media.EsSchema.INDEX_NAME;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.content.Container;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.util.EsPersistenceException;
import org.atlasapi.media.util.Strings;
import org.atlasapi.media.content.ContentIndexer;
import org.atlasapi.media.content.schedule.EsScheduleIndexNames;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;

/**
 */
public class EsContentIndexer extends AbstractIdleService implements ContentIndexer {

    private final Logger log = LoggerFactory.getLogger(EsContentIndexer.class);
    
    private final Node esClient;
    private final EsScheduleIndexNames scheduleNames;
    private final long requestTimeout;
    
    private Set<String> existingIndexes;
    
    public EsContentIndexer(Node esClient) {
        this(esClient, new SystemClock());
    }

    public EsContentIndexer(Node esClient, Clock clock) {
        this(esClient, clock, 60000);
    }

    public EsContentIndexer(Node esClient, Clock clock, long requestTimeout) {
        this.esClient = esClient;
        this.scheduleNames = new EsScheduleIndexNames(esClient, clock);
        this.requestTimeout = requestTimeout;
    }

    @Override
    protected void startUp() throws IOException {
        if (createIndex(INDEX_NAME)) {
            putTopContentMapping(INDEX_NAME);
            putChildContentMapping();
        }
        this.existingIndexes = Sets.newHashSet(scheduleNames.existingIndexNames());
        log.info("Found existing indices {}", existingIndexes);
    }
    
    @Override
    protected void shutDown() throws Exception {
        
    }

    private boolean createIndex(String name) {
        ActionFuture<IndicesExistsResponse> exists = esClient.client().admin().indices().exists(
            Requests.indicesExistsRequest(name)
        );
        if (!timeoutGet(exists).isExists()) {
            log.info("Creating index {}", name);
            timeoutGet(esClient.client().admin().indices().create(Requests.createIndexRequest(name)));
            return true;
        } else {
            log.info("Index {} exists", name);
            return false;
        }
    }

    private void putTopContentMapping(String index) throws IOException, ElasticSearchException {
        log.info("Putting mapping for index {}", index);
        ActionFuture<PutMappingResponse> putMapping = esClient
            .client()
            .admin()
            .indices()
            .putMapping(Requests.putMappingRequest(index)
                .type(EsContent.TOP_LEVEL_TYPE)
                .source(XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject(EsContent.TOP_LEVEL_TYPE)
                            .startObject("_all")
                                .field("enabled").value(false)
                            .endObject()
                            .startObject("properties")
                                .startObject(EsContent.URI)
                                    .field("type").value("string")
                                    .field("index").value("not_analyzed")
                                .endObject()
                                .startObject(EsContent.ID)
                                    .field("type").value("long")
                                    .field("index").value("not_analyzed")
                                .endObject()
                                .startObject(EsContent.TITLE)
                                    .field("type").value("string")
                                    .field("index").value("analyzed")
                                .endObject()
                                .startObject(EsContent.FLATTENED_TITLE)
                                    .field("type").value("string")
                                    .field("index").value("analyzed")
                                .endObject()
                                .startObject(EsContent.PUBLISHER)
                                    .field("type").value("string")
                                    .field("index").value("not_analyzed")
                                .endObject()
                                .startObject(EsContent.SPECIALIZATION)
                                    .field("type").value("string")
                                    .field("index").value("not_analyzed")
                                .endObject()
                                .startObject(EsContent.BROADCASTS)
                                    .field("type").value("nested")
                                    .startObject("properties")
                                        .startObject(EsBroadcast.CHANNEL)
                                            .field("type").value("string")
                                            .field("index").value("not_analyzed")
                                        .endObject()
                                    .endObject()
                                .endObject()
                                .startObject(EsContent.LOCATIONS)
                                    .field("type").value("nested")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                )
            );
        putMapping.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private void putChildContentMapping() throws ElasticSearchException, IOException {
        ActionFuture<PutMappingResponse> putMapping = esClient
            .client()
            .admin()
            .indices()
            .putMapping(Requests.putMappingRequest(INDEX_NAME)
                .type(EsContent.CHILD_TYPE)
                .source(XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject(EsContent.CHILD_TYPE)
                            .startObject("_parent")
                                .field("type").value(EsContent.TOP_LEVEL_TYPE)
                            .endObject()
                            .startObject("_all")
                                .field("enabled").value(false)
                            .endObject()
                            .startObject("properties")
                                .startObject(EsContent.URI)
                                    .field("type").value("string")
                                    .field("index").value("not_analyzed")
                                .endObject()
                                .startObject(EsContent.ID)
                                    .field("type").value("long")
                                    .field("index").value("not_analyzed")
                                .endObject()
                                .startObject(EsContent.TITLE)
                                    .field("type").value("string")
                                    .field("index").value("analyzed")
                                .endObject()
                                .startObject(EsContent.FLATTENED_TITLE)
                                        .field("type").value("string")
                                        .field("index").value("analyzed")
                                .endObject()
                                .startObject(EsContent.PUBLISHER)
                                    .field("type").value("string")
                                    .field("index").value("not_analyzed")
                                .endObject()
                                .startObject(EsContent.SPECIALIZATION)
                                    .field("type").value("string")
                                    .field("index").value("not_analyzed")
                                .endObject()
                                .startObject(EsContent.BROADCASTS)
                                    .field("type").value("nested")
                                    .startObject("properties")
                                        .startObject(EsBroadcast.CHANNEL)
                                            .field("type").value("string")
                                            .field("index").value("not_analyzed")
                                        .endObject()
                                    .endObject()
                                .endObject()
                                .startObject(EsContent.LOCATIONS)
                                    .field("type").value("nested")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                )
            );
        putMapping.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void index(Item item) throws IndexException {
        try {
            EsContent esContent = toEsContent(item);
            
            BulkRequest requests = Requests.bulkRequest();
            IndexRequest mainIndexRequest;
            ParentRef container = item.getContainer();
            if (container != null) {
                fillParentData(esContent, container);
                mainIndexRequest = Requests.indexRequest(INDEX_NAME)
                    .type(EsContent.CHILD_TYPE)
                    .id(getDocId(item))
                    .source(esContent.toMap())
                    .parent(getDocId(container));
            } else {
                mainIndexRequest = Requests.indexRequest(INDEX_NAME)
                    .type(EsContent.TOP_LEVEL_TYPE)
                    .id(getDocId(item))
                    .source(esContent.hasChildren(false).toMap());
            }
            
            requests.add(mainIndexRequest);
            Map<String, ActionRequest<IndexRequest>> scheduleRequests = scheduleIndexRequests(item);
            ensureIndices(scheduleRequests);
            for (ActionRequest<IndexRequest> ir : scheduleRequests.values()) {
                requests.add(ir);
            }
            BulkResponse resp = timeoutGet(esClient.client().bulk(requests));
            log.info("Indexed {} ({}ms, {})", new Object[]{item, resp.getTookInMillis(), scheduleRequests.keySet()});
        } catch (Exception e) {
            throw new IndexException("Error indexing " + item, e);
        }
    }

    private EsContent toEsContent(Item item) {
        return new EsContent()
            .id(item.getId().longValue())
            .uri(item.getCanonicalUri())
            .title(item.getTitle())
            .flattenedTitle(flattenedOrNull(item.getTitle()))
            .parentTitle(item.getTitle())
            .parentFlattenedTitle(flattenedOrNull(item.getTitle()))
            .publisher(item.getPublisher() != null ? item.getPublisher().key() : null)
            .specialization(item.getSpecialization() != null ? item.getSpecialization().name() : null)
            .broadcasts(makeESBroadcasts(item))
            .locations(makeESLocations(item))
            .topics(makeESTopics(item));
    }

    private void ensureIndices(Map<String, ActionRequest<IndexRequest>> scheduleRequests) throws IOException {
        Set<String> missingIndices = Sets.difference(scheduleRequests.keySet(), existingIndexes);
        for (String missingIndex : missingIndices) {
            if (createIndex(missingIndex)) {
                putTopContentMapping(missingIndex);
                existingIndexes.add(missingIndex);
            }
        }
    }

    private Map<String,ActionRequest<IndexRequest>> scheduleIndexRequests(Item item) {
        Multimap<String, EsBroadcast> indicesBroadcasts = HashMultimap.create();
        for (Version version : item.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                EsBroadcast esBroadcast = toEsBroadcast(broadcast);
                Iterable<String> indices = scheduleNames.indexingNamesFor(
                    broadcast.getTransmissionTime(),
                    broadcast.getTransmissionEndTime()
                );
                for (String index : indices) {
                    indicesBroadcasts.put(index, esBroadcast);
                };
            }
        }
        
        Builder<String, ActionRequest<IndexRequest>> requests = ImmutableMap.builder();
        for (Entry<String, Collection<EsBroadcast>> indexBroadcasts : indicesBroadcasts.asMap().entrySet()) {
            requests.put(
                indexBroadcasts.getKey(), 
                Requests.indexRequest(indexBroadcasts.getKey())
                    .type(EsContent.TOP_LEVEL_TYPE)
                    .id(getDocId(item))
                    .source(new EsContent()
                        .id(item.getId().longValue())
                        .uri(item.getCanonicalUri())
                        .publisher(item.getPublisher() != null ? item.getPublisher().key() : null)
                        .broadcasts(indexBroadcasts.getValue())
                        .hasChildren(false)
                        .toMap()
                    )
            );
        }
        
        return requests.build();
    }

    @Override
    public void index(Container container) {
        EsContent indexed = new EsContent()
            .id(container.getId().longValue())
            .uri(container.getCanonicalUri())
            .title(container.getTitle())
            .flattenedTitle(flattenedOrNull(container.getTitle()))
            .parentTitle(container.getTitle())
            .parentFlattenedTitle(flattenedOrNull(container.getTitle()))
            .publisher(container.getPublisher() != null ? container.getPublisher().key() : null)
            .specialization(container.getSpecialization() != null ? container.getSpecialization().name() : null);
        
        if (!container.getChildRefs().isEmpty()) {
            indexed.hasChildren(Boolean.TRUE);
            indexChildrenData(container);
        } else {
            indexed.hasChildren(Boolean.FALSE);
        }
        IndexRequest request = Requests.indexRequest(INDEX_NAME)
            .type(EsContent.TOP_LEVEL_TYPE)
            .id(getDocId(container))
            .source(indexed.toMap());
        timeoutGet(esClient.client().index(request));
    }


    private String flattenedOrNull(String string) {
        return string != null ? Strings.flatten(string) : null;
    }
    
    private Collection<EsBroadcast> makeESBroadcasts(Item item) {
        Collection<EsBroadcast> esBroadcasts = new LinkedList<EsBroadcast>();
        for (Version version : item.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (broadcast.isActivelyPublished()) {
                    esBroadcasts.add(toEsBroadcast(broadcast));
                }
            }
        }
        return esBroadcasts;
    }

    private EsBroadcast toEsBroadcast(Broadcast broadcast) {
        return new EsBroadcast()
            .id(broadcast.getSourceId())
            .channel(broadcast.getBroadcastOn())
            .transmissionTime(toUtc(broadcast.getTransmissionTime()).toDate())
            .transmissionEndTime(toUtc(broadcast.getTransmissionEndTime()).toDate())
            .transmissionTimeInMillis(toUtc(broadcast.getTransmissionTime()).getMillis())
            .repeat(broadcast.getRepeat() != null ? broadcast.getRepeat() : false);
    }

    private DateTime toUtc(DateTime transmissionTime) {
        return transmissionTime.toDateTime(DateTimeZones.UTC);
    }

    private Collection<EsLocation> makeESLocations(Item item) {
        Collection<EsLocation> esLocations = new LinkedList<EsLocation>();
        for (Version version : item.getVersions()) {
            for (Encoding encoding : version.getManifestedAs()) {
                for (Location location : encoding.getAvailableAt()) {
                    if (location.getPolicy() != null 
                        && location.getPolicy().getAvailabilityStart() != null
                        && location.getPolicy().getAvailabilityEnd() != null) {
                            esLocations.add(toEsLocation(location.getPolicy()));
                    }
                }
            }
        }
        return esLocations;
    }

    private EsLocation toEsLocation(Policy policy) {
        return new EsLocation()
            .availabilityTime(toUtc(policy.getAvailabilityStart()).toDate())
            .availabilityEndTime(toUtc(policy.getAvailabilityEnd()).toDate());
    }

    private Collection<EsTopicMapping> makeESTopics(Item item) {
        Collection<EsTopicMapping> esTopics = new LinkedList<EsTopicMapping>();
        for (TopicRef topic : item.getTopicRefs()) {
            esTopics.add(new EsTopicMapping().id(topic.getTopic().longValue()));
        }
        return esTopics;
    }

    private void fillParentData(EsContent child, ParentRef parent) {
        Map<String, Object> indexedContainer = trySearchParent(parent);
        if (indexedContainer != null) {
            Object title = indexedContainer.get(EsContent.TITLE);
            child.parentTitle(title != null ? title.toString() : null);
            Object flatTitle = indexedContainer.get(EsContent.FLATTENED_TITLE);
            child.parentFlattenedTitle(flatTitle != null ? flatTitle.toString() : null);
        }
    }

    private void indexChildrenData(Container parent) {
        BulkRequest bulk = Requests.bulkRequest();
        for (ChildRef child : parent.getChildRefs()) {
            Map<String, Object> indexedChild = trySearchChild(parent, child);
            if (indexedChild != null) {
                if (parent.getTitle() != null) {
                    indexedChild.put(EsContent.PARENT_TITLE, parent.getTitle());
                    indexedChild.put(EsContent.PARENT_FLATTENED_TITLE, Strings.flatten(parent.getTitle()));
                    bulk.add(Requests.indexRequest(INDEX_NAME).
                            type(EsContent.CHILD_TYPE).
                            parent(getDocId(parent)).
                            id(getDocId(child)).
                            source(indexedChild));
                }
            }
        }
        if (bulk.numberOfActions() > 0) {
            BulkResponse response = timeoutGet(esClient.client().bulk(bulk));
            if (response.hasFailures()) {
                throw new EsPersistenceException("Failed to index children for container: " + getDocId(parent));
            }
        }
    }

    private String getDocId(ChildRef child) {
        return String.valueOf(child.getId());
    }
    
    private String getDocId(Content content) {
        return String.valueOf(content.getId());
    }
    
    private String getDocId(ParentRef container) {
        return String.valueOf(container.getId());
    }

    private Map<String, Object> trySearchParent(ParentRef parent) {
        GetRequest request = Requests.getRequest(INDEX_NAME).id(getDocId(parent));
        GetResponse response = timeoutGet(esClient.client().get(request));
        if (response.isExists()) {
            return response.getSource();
        } else {
            return null;
        }
    }

    private Map<String, Object> trySearchChild(Container parent, ChildRef child) {
        try {
            GetRequest request = Requests.getRequest(INDEX_NAME)
                    .parent(getDocId(parent))
                    .id(getDocId(child));
            GetResponse response = timeoutGet(esClient.client().get(request));
            if (response.isExists()) {
                return response.getSource();
            } else {
                return null;
            }
        } catch (NoShardAvailableActionException ex) {
            return null;
        }
    }
    
    private <T> T timeoutGet(ActionFuture<T> future) {
        return future.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

}
