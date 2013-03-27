package org.atlasapi.media.util;

import java.util.Map;

import org.elasticsearch.action.admin.indices.status.IndexStatus;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;


public class ElasticSearchHelper {
    
    private ElasticSearchHelper() {}
    
    public static void refresh(Node esClient) {
        Map<String, IndexStatus> indices = esClient.client().admin().indices()
            .status(Requests.indicesStatusRequest((String[]) null))
            .actionGet()
            .getIndices();
        esClient.client().admin().indices()
            .prepareRefresh(indices.keySet().toArray(new String[]{}))
            .execute()
            .actionGet();
    }
    
}
