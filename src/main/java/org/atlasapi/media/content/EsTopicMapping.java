package org.atlasapi.media.content;

import org.atlasapi.media.util.EsObject;

/**
 */
public class EsTopicMapping extends EsObject {

    public final static String ID = "id";

    public EsTopicMapping id(Long id) {
        properties.put(ID, id);
        return this;
    }
}
