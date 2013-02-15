package org.atlasapi.media.content;

import java.util.Date;

import org.atlasapi.media.util.EsObject;

/**
 */
public class EsLocation extends EsObject {
    
    public final static String AVAILABILITY_TIME = "availabilityTime";
    public final static String AVAILABILITY_END_TIME = "availabilityEndTime";
    
    
    public EsLocation availabilityTime(Date availabilityTime) {
        properties.put(AVAILABILITY_TIME, availabilityTime);
        return this;
    }
    
    public EsLocation availabilityEndTime(Date availabilityEndTime) {
        properties.put(AVAILABILITY_END_TIME, availabilityEndTime);
        return this;
    }
    
}
