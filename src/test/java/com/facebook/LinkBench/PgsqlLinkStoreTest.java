package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import com.facebook.LinkBench.testtypes.PgsqlTest;

/**
 * Test the Postgres LinkStore implementation.
 *
 * Assumes that the database specified by the testDB field has been created
 * with permissions for a user/pass linkbench/linkbench to create tables, select,
 * insert, delete, etc.
 */

public class PgsqlLinkStoreTest extends LinkStoreTestBase {

  private Connection conn;

  /** Properties for last initStore call */
  private Properties currProps;

  protected long getIDCount() {
    // Make test smaller so that it doesn't take too long
    return 5000;
  }

  protected int getRequestCount() {
    // Fewer requests to keep test quick
    return 20000;
  }

  protected Properties basicProps() {
    Properties props = super.basicProps();
    PgsqlTestConfig.fillPgsqlTestServerProps(props);
    return props;
  }


  protected void initStore(Properties props) throws IOException, Exception {
    this.currProps = (Properties)props.clone();
    if (conn != null) {
      conn.close();
    }
    conn = PgsqlTestConfig.createConnection(testDB);
    PgsqlTestConfig.dropTestTables(conn, testDB);
    PgsqlTestConfig.createTestTables(conn, testDB);
  }


  public DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
    DummyLinkStore result = new DummyLinkStore(new LinkStorePgsql());
    if (initialize) {
      result.initialize(currProps, Phase.REQUEST, 0);
    }
    return result;
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    PgsqlTestConfig.dropTestTables(conn, testDB);
    conn.close();
  }

}
