package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import com.facebook.LinkBench.testtypes.PgsqlTest;

public class PgsqlNodeStoreTest extends NodeStoreTestBase {

  Connection conn;
  Properties currProps;

  protected Properties basicProps() {
    Properties props = super.basicProps();
    PgsqlTestConfig.fillPgsqlTestServerProps(props);
    return props;
  }

  protected void initNodeStore(Properties props) throws Exception, IOException {
    currProps = props;
    conn = PgsqlTestConfig.createConnection(testDB);
    PgsqlTestConfig.dropTestTables(conn, testDB);
    PgsqlTestConfig.createTestTables(conn, testDB);
  }

  protected NodeStore getNodeStoreHandle(boolean initialize) throws Exception, IOException {
    DummyLinkStore result = new DummyLinkStore(new LinkStorePgsql());
    if (initialize) {
      result.initialize(currProps, Phase.REQUEST, 0);
    }
    return result;
  }

}
