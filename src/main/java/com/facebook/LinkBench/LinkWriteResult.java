
package com.facebook.LinkBench;

public enum LinkWriteResult {
    LINK_INSERT,     // Done via an insert
    LINK_UPDATE,     // Done via an update
    LINK_NO_CHANGE,  // Not done because data didn't change
    LINK_NOT_DONE    // Operation wasn't done. Retry.
}

