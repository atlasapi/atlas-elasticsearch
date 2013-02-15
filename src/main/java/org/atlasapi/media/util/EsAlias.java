package org.atlasapi.media.util;



public class EsAlias extends EsObject {

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
