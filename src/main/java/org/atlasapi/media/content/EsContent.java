package org.atlasapi.media.content;

import java.util.Collection;

import org.atlasapi.media.util.EsObject;

import com.google.common.collect.Iterables;

/**
 */
public class EsContent extends EsObject {

    public final static String TOP_LEVEL_TYPE = "container";
    public final static String CHILD_TYPE = "child_item";
    public final static String TOP_ITEM_TYPE = "top_item";

    public final static String ID = "id";
    public final static String URI = "uri";
    public final static String TITLE = "title";
    public final static String FLATTENED_TITLE = "flattenedTitle";
    public final static String PARENT_TITLE = "parentTitle";
    public final static String PARENT_FLATTENED_TITLE = "parentFlattenedTitle";
    public final static String PUBLISHER = "publisher";
    public final static String SPECIALIZATION = "specialization";
    public final static String BROADCASTS = "broadcasts";
    public final static String LOCATIONS = "locations";
    public final static String TOPICS = "topics";
    public final static String HAS_CHILDREN = "hasChildren";

    public EsContent id(long id) {
        properties.put(ID, id);
        return this;
    }
    
    public EsContent uri(String uri) {
        properties.put(URI, uri);
        return this;
    }

    public EsContent title(String title) {
        properties.put(TITLE, title);
        return this;
    }
    
    public EsContent flattenedTitle(String flattenedTitle) {
        properties.put(FLATTENED_TITLE, flattenedTitle);
        return this;
    }

    public EsContent parentTitle(String parentTitle) {
        properties.put(PARENT_TITLE, parentTitle);
        return this;
    }
    
    public EsContent parentFlattenedTitle(String parentFlattenedTitle) {
        properties.put(PARENT_FLATTENED_TITLE, parentFlattenedTitle);
        return this;
    }
    
    public EsContent publisher(String publisher) {
        properties.put(PUBLISHER, publisher);
        return this;
    }

    public EsContent specialization(String specialization) {
        properties.put(SPECIALIZATION, specialization);
        return this;
    }

    public EsContent broadcasts(Collection<EsBroadcast> broadcasts) {
        properties.put(BROADCASTS, Iterables.transform(broadcasts, TO_MAP));
        return this;
    }

    public EsContent locations(Collection<EsLocation> locations) {
        properties.put(LOCATIONS, Iterables.transform(locations, TO_MAP));
        return this;
    }

    public EsContent topics(Collection<EsTopicMapping> topics) {
        properties.put(TOPICS, Iterables.transform(topics, TO_MAP));
        return this;
    }
    
    public EsContent hasChildren(Boolean hasChildren) {
        properties.put(HAS_CHILDREN, hasChildren);
        return this;
    }
}
