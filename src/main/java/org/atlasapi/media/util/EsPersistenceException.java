package org.atlasapi.media.util;

/**
 */
public class EsPersistenceException extends RuntimeException {

    public EsPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public EsPersistenceException(String message) {
        super(message);
    }
}
