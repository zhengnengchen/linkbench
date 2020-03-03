/*
 * Copyright 2012, Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench;

import java.util.List;
import java.util.Properties;

/**
 * An abstract class for storing both nodes and edges
 * @author tarmstrong
 */
public abstract class GraphStore extends LinkStore implements NodeStore {
  protected boolean check_count = false;
  protected Level debuglevel;
  protected Logger logger;

  public static final String CONFIG_BULK_INSERT_COUNT = "bulk_insert_count";
  public static final String CONFIG_BULK_INSERT_KB = "bulk_insert_kb";

  // Max number of Nodes, Links and Counts in a bulk load insert
  protected int bulkInsertCount = 1024;

  // Max size of Nodes in a bulk load insert. Not used to limit Link or Count
  // because they are smaller. When 0 there is no limit.
  protected int bulkInsertKB = 0;

  protected Integer retry_add_link = 0;
  protected Integer retry_update_link = 0;
  protected Integer retry_delete_link = 0;
  protected Integer retry_get_link = 0;
  protected Integer retry_multigetlinks = 0;
  protected Integer retry_get_link_list = 0;
  protected Integer retry_count_links = 0;
  protected Integer retry_add_bulk_links = 0;
  protected Integer retry_add_bulk_counts = 0;
  protected Integer retry_add_node = 0;
  protected Integer retry_bulk_add_nodes = 0;
  protected Integer retry_get_node = 0;
  protected Integer retry_update_node = 0;
  protected Integer retry_delete_node = 0;

  protected Integer max_add_link = 0;
  protected Integer max_update_link = 0;
  protected Integer max_delete_link = 0;
  protected Integer max_get_link = 0;
  protected Integer max_multigetlinks = 0;
  protected Integer max_get_link_list = 0;
  protected Integer max_count_links = 0;
  protected Integer max_add_bulk_links = 0;
  protected Integer max_add_bulk_counts = 0;
  protected Integer max_add_node = 0;
  protected Integer max_bulk_add_nodes = 0;
  protected Integer max_get_node = 0;
  protected Integer max_update_node = 0;
  protected Integer max_delete_node = 0;

  protected int retry_add_to_upd = 0;
  protected int retry_upd_to_add = 0;

  public GraphStore() {
    logger = Logger.getLogger();
  }

  public void initialize(Properties props, Phase phase, int threadId) {
    debuglevel = ConfigUtil.getDebugLevel(props);

    if (props.containsKey(Config.CHECK_COUNT))
      check_count = ConfigUtil.getBool(props, Config.CHECK_COUNT);

    if (props.containsKey(CONFIG_BULK_INSERT_COUNT))
      bulkInsertCount = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_COUNT);

    if (props.containsKey(CONFIG_BULK_INSERT_KB))
      bulkInsertKB = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_KB);
  }

  public int bulkLoadBatchSize() { return bulkInsertCount; }
  public int bulkLoadBatchKB() { return bulkInsertKB; }

  /** Provide generic implementation */
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];
    int i = 0;
    for (Node node: nodes) {
      long id = addNode(dbid, node);
      ids[i++] = id;
    }
    return ids;
  }

  public void printMetrics() {
    logger.info("SQL Link total retry: " +
                retry_add_link + " add, " +
                retry_update_link + " update, " +
                retry_delete_link + " delete, " +
                retry_get_link + " get, " +
                retry_multigetlinks + " multiget, " +
                retry_get_link_list + " get_link_list, " +
                retry_count_links + " count, " +
                retry_add_bulk_links + " add_bulk_links, " +
                retry_add_bulk_counts + " add_bulk_counts");
    logger.info("SQL Link max retry: " +
                max_add_link + " add, " +
                max_update_link + " update, " +
                max_delete_link + " delete, " +
                max_get_link + " get, " +
                max_multigetlinks + " multiget, " +
                max_get_link_list + " get_link_list, " +
                max_count_links + " count, " +
                max_add_bulk_links + " add_bulk_links, " +
                max_add_bulk_counts + " add_bulk_counts");
    logger.info("SQL Link other: " +
                retry_add_to_upd + " add_to_upd, " +
                retry_upd_to_add + " upd_to_add");
    logger.info("SQL Node total retry: " +
                retry_add_node + " add, " +
                retry_bulk_add_nodes + " add_bulk, " +
                retry_get_node + " get, " +
                retry_update_node + " update, " +
                retry_delete_node + " delete");
    logger.info("SQL Node max retry: " +
                max_add_node + " add, " +
                max_bulk_add_nodes + " add_bulk, " +
                max_get_node + " get, " +
                max_update_node + " update, " +
                max_delete_node + " delete");
  }
}
