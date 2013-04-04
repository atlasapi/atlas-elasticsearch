package org.atlasapi.media.content;

import org.atlasapi.media.util.EsObject;

import com.google.common.collect.ImmutableMap;

public class EsTopicMapping extends EsObject {

    public final static String TOPIC = "topic"; 
    public final static String ID = "id";
    public static final String TOPIC_ID = TOPIC + "." + ID;
    public static final String SUPERVISED = "supervised";
    public static final String WEIGHTING = "weighting";

    public EsTopicMapping topicId(Long id) {
        properties.put(TOPIC, ImmutableMap.of(ID, id));
        return this;
    }
    
    public EsTopicMapping supervised(Boolean supervised) {
        properties.put(SUPERVISED, supervised);
        return this;
    }
    
    public EsTopicMapping weighting(Float weighting) {
        properties.put(WEIGHTING, weighting);
        return this;
    }
    
}
