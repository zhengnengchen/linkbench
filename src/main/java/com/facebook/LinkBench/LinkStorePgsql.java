/* * LinkStore for PostgreSQL
 * Author : woonhak.kang (woonhak.kang@gmail.com)
 * Date : 01/26/2016
 * Author: Mark Callaghan
 * Date : Feb 2020
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

public class LinkStorePgsql extends LinkStoreSql {

  /* PostgreSQL database server configuration keys */
  public static final String CONFIG_BULK_INSERT_BATCH = "postgres_bulk_insert_batch";

  public LinkStorePgsql() {
    super();
  }

  public LinkStorePgsql(Properties props) throws IOException, Exception {
    super();
    initialize(props, Phase.LOAD, 0);
  }

  public void initialize(Properties props, Phase currentPhase, int threadId) {
    super.initialize(props, currentPhase, threadId);

    if (port == null || port.equals("")) port = "5432"; //use default port
    phase = currentPhase;

    if (props.containsKey(CONFIG_BULK_INSERT_BATCH)) {
      bulkInsertSize = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_BATCH);
    }
  }

  protected PreparedStatement makeAddLinkIncCountPS() throws SQLException {
    String sql = "INSERT INTO " + init_dbid + "." + counttable +
                 "(id, link_type, count, time, version) " +
                 "VALUES (?, ?, ?, ?, 0) " +
                 "ON CONFLICT ON CONSTRAINT " + counttable + "_pkey DO UPDATE SET " +
                 " count = " + init_dbid + "." + counttable +".count + ?" +
                 ", version = " + init_dbid + "." + counttable +".version + 1 " +
                 ", time = ?";

    logger.debug("addLinkIncCount PS: " + sql);
    return conn_ac0.prepareStatement(sql);
  }

  protected PreparedStatement makeGetLinkListPS() throws SQLException {
    String sql = "SELECT id1, id2, link_type," +
                 " visibility, data, version, time" +
                 " FROM " + init_dbid + "." + linktable +
                 " WHERE id1 = ? AND link_type = ? " +
                 " AND time >= ?" +
                 " AND time <= ?" +
                 " AND visibility = " + LinkStore.VISIBILITY_DEFAULT +
                 " ORDER BY time DESC" +
                 " LIMIT ? OFFSET ?";
    logger.debug("getLinkList PS: " + sql);
    return conn_ac1.prepareStatement(sql);
  }

  // This hardwires Linkbench to use the database "linkbench"
  protected String getJdbcUrl() {
    return "jdbc:postgresql://"+ host + ":" + port + "/linkbench";
  }

  protected String getJdbcClassName() {
    return "org.postgresql.Driver";
  }

  protected String getJdbcOptions() {
    // TODO are these valid for PG?
    return "?elideSetAutoCommits=true" +
           "&useLocalTransactionState=true" +
           "&allowMultiQueries=true" +
           "&useLocalSessionState=true" +
           "&useAffectedRows=true";
  }

  /**
   * Set of all JDBC SQLState strings that indicate a transient error
   * that should be handled by retrying
   */
  protected HashSet<String> populateRetrySQLStates() {
    // TODO are there more?
    HashSet<String> states = new HashSet<String>();
    states.add("41000"); // ER_LOCK_WAIT_TIMEOUT
    states.add("40001"); // ER_LOCK_DEADLOCK
    return states;
  }

  protected boolean isDupKeyError(SQLException ex) {
    // 23505 is unique_violation, see https://www.postgresql.org/docs/12/errcodes-appendix.html
    return ex.getSQLState().equals("23505");
  }

  @Override
  public void resetNodeStore(String dbid, long startID) throws SQLException {
    checkNodeTableConfigured();
    // Truncate table deletes all data and allows us to reset autoincrement
    stmt_ac1.execute(String.format("TRUNCATE TABLE %s.%s;", dbid, nodetable));
    
    stmt_ac1.execute(String.format("ALTER SEQUENCE %s.%s_id_seq RESTART %d;",
                                   dbid, nodetable, startID));
  }

}
