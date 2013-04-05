package org.atlasapi.media.content;

import java.io.IOException;

import org.atlasapi.media.entity.TopicRef.Relationship;
import org.atlasapi.media.util.EsObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.google.common.collect.ImmutableMap;

public class EsTopicMapping extends EsObject {
    
    public static final XContentBuilder getMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
                .startObject(EsTopicMapping.TOPIC)
                    .field("type").value("nested")
                    .startObject(EsTopicMapping.ID)
                        .field("type").value("long")
                    .endObject()
                .endObject()
                .startObject(EsTopicMapping.SUPERVISED)
                    .field("type").value("boolean")
                .endObject()
                .startObject(EsTopicMapping.WEIGHTING)
                    .field("type").value("float")
                .endObject()
                .startObject(EsTopicMapping.RELATIONSHIP)
                    .field("type").value("string")
                    .field("index").value("not_analyzed")
                .endObject()
            .endObject();
    }

    public final static String TOPIC = "topic"; 
    public final static String ID = "id";
    public static final String TOPIC_ID = TOPIC + "." + ID;
    public static final String SUPERVISED = "supervised";
    public static final String WEIGHTING = "weighting";
    public static final String RELATIONSHIP = "relationship";

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
    
    public EsTopicMapping relationship(Relationship relationship) {
        if (relationship != null) {
            properties.put(RELATIONSHIP, relationship.toString());
        }
        return this;
    }
    
}
