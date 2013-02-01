package org.elasticsearch.plugin.degraphmalizer;

import java.util.Map;

/**
 * MBean interface for JMX monitoring.
 */
public interface UpdaterManagerMBean {
    Map<String, Integer> getQueueSizes();

    boolean flushQueue(String indexName);
}
