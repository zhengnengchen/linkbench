package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import com.facebook.LinkBench.testtypes.PgsqlTest;

public class PgsqlGraphStoreTest extends GraphStoreTestBase {

  private Properties props;
  private Connection conn;

  protected void initStore(Properties props) throws IOException, Exception {
    this.props = props;
    this.conn = PgsqlTestConfig.createConnection(testDB);
    PgsqlTestConfig.dropTestTables(conn, testDB);
    PgsqlTestConfig.createTestTables(conn, testDB);
  }

  protected long getIDCount() {
    // Make quicker
    return 1000;
  }

  protected int getRequestCount() {
    // Make quicker, enough requests that we can reasonably check
    // that operation percentages are about about right
    return 20000;
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    PgsqlTestConfig.dropTestTables(conn, testDB);
  }

  protected Properties basicProps() {
    Properties props = super.basicProps();
    PgsqlTestConfig.fillPgsqlTestServerProps(props);
    return props;
  }


  protected DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
    DummyLinkStore result = new DummyLinkStore(new LinkStorePgsql());
    if (initialize) {
      result.initialize(props, Phase.REQUEST, 0);
    }
    return result;
  }

}
