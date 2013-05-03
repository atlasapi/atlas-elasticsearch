package org.atlasapi.media.content;

import java.io.IOException;
import java.util.Collection;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.util.EsAlias;
import org.atlasapi.media.util.EsObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;

public class EsContent extends EsObject {

    public final static String TOP_LEVEL_CONTAINER = "container";
    public final static String CHILD_ITEM = "child_item";
    public final static String TOP_LEVEL_ITEM = "top_item";

    public final static String ID = "id";
    public final static String TYPE = "type";
    public final static String SOURCE = "source";
    public final static String ALIASES = "aliases";
    public final static String TITLE = "title";
    public final static String FLATTENED_TITLE = "flattenedTitle";
    public final static String PARENT_TITLE = "parentTitle";
    public final static String PARENT_FLATTENED_TITLE = "parentFlattenedTitle";
    public final static String SPECIALIZATION = "specialization";
    public final static String BROADCASTS = "broadcasts";
    public final static String LOCATIONS = "locations";
    public final static String TOPICS = "topics";
    public final static String HAS_CHILDREN = "hasChildren";
    
    public static final XContentBuilder getTopLevelMapping(String type) throws IOException {
        return addCommonProperties(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(type)
                    .startObject("_all")
                        .field("enabled").value(false)
                    .endObject()
                    .startObject("properties"))
                    .endObject()
                .endObject()
            .endObject();
    }
     
    public static final XContentBuilder getChildMapping() throws IOException {
        return addCommonProperties(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(EsContent.CHILD_ITEM)
                    .startObject("_parent")
                        .field("type").value(EsContent.TOP_LEVEL_CONTAINER)
                    .endObject()
                    .startObject("_all")
                        .field("enabled").value(false)
                    .endObject()
                    .startObject("properties"))
                    .endObject()
                .endObject()
            .endObject();
    }
    

    public static XContentBuilder getScheduleMapping() throws IOException {
        return addSheduleOnlyProperties(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(EsContent.TOP_LEVEL_ITEM)
                    .startObject("_all")
                        .field("enabled").value(false)
                    .endObject()
                    .startObject("properties"))
                    .endObject()
                .endObject()
            .endObject();
    }

    private static XContentBuilder addCommonProperties(XContentBuilder obj) throws IOException {
        return addSheduleOnlyProperties(obj
            .startObject(EsContent.TYPE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
            .endObject()
            .startObject(ALIASES)
                .field("type").value("nested")
                .rawField("properties", EsAlias.getMapping().bytes())
            .endObject()
            .startObject(EsContent.TITLE)
                .field("type").value("string")
                .field("index").value("analyzed")
            .endObject()
            .startObject(EsContent.FLATTENED_TITLE)
                    .field("type").value("string")
                    .field("index").value("analyzed")
            .endObject()
            .startObject(EsContent.SOURCE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
            .endObject()
            .startObject(EsContent.SPECIALIZATION)
                .field("type").value("string")
                .field("index").value("not_analyzed")
            .endObject()
            .startObject(EsContent.TOPICS)
                .field("type").value("nested")
                .rawField("properties", EsTopicMapping.getMapping().bytes())
            .endObject()
            .startObject(EsContent.LOCATIONS)
                .field("type").value("nested")
            .endObject());
    }

    private static XContentBuilder addSheduleOnlyProperties(XContentBuilder obj) throws IOException {
        return obj.startObject(EsContent.ID)
                .field("type").value("long")
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
            .endObject();
    }

    public EsContent id(long id) {
        properties.put(ID, id);
        return this;
    }
    
    public EsContent type(ContentType type) {
        properties.put(TYPE, type.getKey());
        return this;
    }
    
    public EsContent aliases(Iterable<Alias> aliases) {
        properties.put(ALIASES, Iterables.transform(aliases, Functions.compose(TO_MAP, EsAlias.toEsAlias())));
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
    
    public EsContent source(String publisher) {
        properties.put(SOURCE, publisher);
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
