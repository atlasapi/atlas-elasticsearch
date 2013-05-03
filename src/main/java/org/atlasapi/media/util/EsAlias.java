package org.atlasapi.media.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.media.entity.Alias;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.google.common.base.Function;

public class EsAlias extends EsObject {
    
    public static final XContentBuilder getMapping() throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(EsAlias.NAMESPACE)
                        .field("type").value("string")
                        .field("index").value("not_analyzed")
                    .endObject()
                    .startObject(EsAlias.VALUE)
                        .field("type").value("string")
                        .field("index").value("not_analyzed")
                    .endObject()
                .endObject();
    }
    
    private static final Function<Alias, EsAlias> TO_ALIAS =
            new Function<Alias, EsAlias>() {
                @Override
                public EsAlias apply(Alias input) {
                    return EsAlias.valueOf(input);
                }
            };
            
    public static final Function<Alias, EsAlias> toEsAlias() {
        return TO_ALIAS;
    }
    
    public static final EsAlias valueOf(Alias alias) {
        checkNotNull(alias);
        return new EsAlias()
            .namespace(alias.getNamespace())
            .value(alias.getValue());
    }

    public static final String NAMESPACE = "namespace";
    public static final String VALUE = "value";
    
    public EsAlias namespace(String namespace) {
        properties.put(NAMESPACE, namespace);
        return this;
    }
    
    public EsAlias value(String value) {
        properties.put(VALUE, value);
        return this;
    }
    
}
