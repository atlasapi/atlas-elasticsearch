package org.atlasapi.media.content;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.media.EsSchema.CONTENT_INDEX;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.content.schedule.EsScheduleIndexNames;
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
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;

/**
 */
public class EsContentIndexer extends AbstractIdleService implements ContentIndexer {

    private final Logger log = LoggerFactory.getLogger(EsContentIndexer.class);
    
    private final Node esClient;
    private final EsScheduleIndexNames scheduleNames;
    private final long requestTimeout;
    
    private Set<String> existingIndexes;

    private String index;
    
    public EsContentIndexer(Node esClient, String index, Clock clock, long requestTimeout) {
        this.esClient = checkNotNull(esClient);
        this.scheduleNames = new EsScheduleIndexNames(esClient, clock);
        this.requestTimeout = requestTimeout;
        this.index = checkNotNull(index);
    }

    @Override
    protected void startUp() throws IOException {
        if (createIndex(index)) {
            putTypeMappings();
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
            try {
                log.info("Creating index {}", name);
                timeoutGet(esClient.client().admin().indices().create(Requests.createIndexRequest(name)));
            } catch (IndexAlreadyExistsException iaee) {
                log.info("Already exists: {}", name);
                return false;
            }
            return true;
        } else {
            log.info("Index {} exists", name);
            return false;
        }
    }
    
    private void putTypeMappings() throws IOException {
        log.info("Putting mapping for type {}", EsContent.TOP_LEVEL_CONTAINER);
        doMappingRequest(Requests.putMappingRequest(index)
            .type(EsContent.TOP_LEVEL_CONTAINER)
            .source(EsContent.getTopLevelMapping(EsContent.TOP_LEVEL_CONTAINER)));
        log.info("Putting mapping for type {}", EsContent.TOP_LEVEL_ITEM);
        doMappingRequest(Requests.putMappingRequest(index)
            .type(EsContent.TOP_LEVEL_ITEM)
            .source(EsContent.getTopLevelMapping(EsContent.TOP_LEVEL_ITEM)));
        log.info("Putting mapping for type {}", EsContent.CHILD_ITEM);
        doMappingRequest(Requests.putMappingRequest(index)
                .type(EsContent.CHILD_ITEM)
                .source(EsContent.getChildMapping()));
    }

    private void doMappingRequest(PutMappingRequest req) {
        try {
            timeoutGet(esClient.client().admin().indices().putMapping(req));
        } catch (MergeMappingException mme) {
            log.info("Merge Mapping Exception: {}/{}", req.indices(), req.type());
        }
        
    }
    
    @Override
    public void index(Content content) throws IndexException {
        try {
            content.accept(new ContentVisitorAdapter<Void>() {
                @Override
                protected Void visitItem(Item item) {
                    indexItem(item);
                    return null;
                }
                @Override
                protected Void visitContainer(Container container) {
                    indexContainer(container);
                    return null;
                }
            });
        } catch (RuntimeIndexException rie) {
            throw new IndexException(rie.getMessage(), rie.getCause());
        }
    };
    
    private class RuntimeIndexException extends RuntimeException {

        public RuntimeIndexException(String message, Throwable cause) {
            super(message, cause);
        }
        
    }

    private void indexItem(Item item) {
        try {
            EsContent esContent = toEsContent(item);
            
            BulkRequest requests = Requests.bulkRequest();
            IndexRequest mainIndexRequest;
            ParentRef container = item.getContainer();
            if (container != null) {
                fillParentData(esContent, container);
                mainIndexRequest = Requests.indexRequest(CONTENT_INDEX)
                    .type(EsContent.CHILD_ITEM)
                    .id(getDocId(item))
                    .source(esContent.toMap())
                    .parent(getDocId(container));
            } else {
                mainIndexRequest = Requests.indexRequest(CONTENT_INDEX)
                    .type(EsContent.TOP_LEVEL_ITEM)
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
            throw new RuntimeIndexException("Error indexing " + item, e);
        }
    }

    private EsContent toEsContent(Item item) {
        return new EsContent()
            .id(item.getId().longValue())
            .type(ContentType.fromContent(item).get())
            .source(item.getPublisher() != null ? item.getPublisher().key() : null)
            .aliases(item.getAliases())
            .title(item.getTitle())
            .flattenedTitle(flattenedOrNull(item.getTitle()))
            .parentTitle(item.getTitle())
            .parentFlattenedTitle(flattenedOrNull(item.getTitle()))
            .specialization(item.getSpecialization() != null ? item.getSpecialization().name() : null)
            .broadcasts(makeESBroadcasts(item))
            .locations(makeESLocations(item))
            .topics(makeESTopics(item));
    }

    private void ensureIndices(Map<String, ActionRequest<IndexRequest>> scheduleRequests) throws IOException {
        Set<String> missingIndices = Sets.difference(scheduleRequests.keySet(), existingIndexes);
        for (String missingIndex : missingIndices) {
            if (createIndex(missingIndex)) {
                doMappingRequest(Requests.putMappingRequest(missingIndex)
                        .type(EsContent.TOP_LEVEL_ITEM)
                        .source(EsContent.getScheduleMapping()));
                refresh(missingIndex);
                existingIndexes.add(missingIndex);
            }
        }
    }

    private void refresh(String missingIndex) {
        timeoutGet(esClient.client().admin().indices().refresh(Requests.refreshRequest(missingIndex)));
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
                    .type(EsContent.TOP_LEVEL_ITEM)
                    .id(getDocId(item))
                    .source(new EsContent()
                        .id(item.getId().longValue())
                        .source(item.getPublisher() != null ? item.getPublisher().key() : null)
                        .broadcasts(indexBroadcasts.getValue())
                        .hasChildren(false)
                        .toMap()
                    )
            );
        }
        
        return requests.build();
    }

    private void indexContainer(Container container) {
        EsContent indexed = new EsContent()
            .id(container.getId().longValue())
            .type(ContentType.fromContent(container).get())
            .source(container.getPublisher() != null ? container.getPublisher().key() : null)
            .aliases(container.getAliases())
            .title(container.getTitle())
            .flattenedTitle(flattenedOrNull(container.getTitle()))
            .parentTitle(container.getTitle())
            .parentFlattenedTitle(flattenedOrNull(container.getTitle()))
            .specialization(container.getSpecialization() != null ? container.getSpecialization().name() : null)
            .topics(makeESTopics(container));
        if (!container.getChildRefs().isEmpty()) {
            indexed.hasChildren(Boolean.TRUE);
            indexChildrenData(container);
        } else {
            indexed.hasChildren(Boolean.FALSE);
        }
        IndexRequest request = Requests.indexRequest(CONTENT_INDEX)
            .type(EsContent.TOP_LEVEL_CONTAINER)
            .id(getDocId(container))
            .source(indexed.toMap());
        timeoutGet(esClient.client().index(request));
        log.info("Indexed {}", new Object[]{container});
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

    private Collection<EsTopicMapping> makeESTopics(Content content) {
        Collection<EsTopicMapping> esTopics = new LinkedList<EsTopicMapping>();
        for (TopicRef topic : content.getTopicRefs()) {
            esTopics.add(new EsTopicMapping()
                .topicId(topic.getTopic().longValue())
                .supervised(topic.isSupervised())
                .weighting(topic.getWeighting())
                .relationship(topic.getRelationship()));
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
                    bulk.add(Requests.indexRequest(CONTENT_INDEX).
                            type(EsContent.CHILD_ITEM).
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
        GetRequest request = Requests.getRequest(CONTENT_INDEX).id(getDocId(parent));
        GetResponse response = timeoutGet(esClient.client().get(request));
        if (response.isExists()) {
            return response.getSource();
        } else {
            return null;
        }
    }

    private Map<String, Object> trySearchChild(Container parent, ChildRef child) {
        try {
            GetRequest request = Requests.getRequest(CONTENT_INDEX)
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
        try {
            return future.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (ElasticSearchException ese) {
            Throwable root = Throwables.getRootCause(ese);
            Throwables.propagateIfInstanceOf(root, ElasticSearchException.class);
            throw Throwables.propagate(ese);
        }
    }

}
