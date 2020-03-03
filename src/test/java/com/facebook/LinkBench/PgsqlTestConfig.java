package com.facebook.LinkBench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PgsqlTestConfig {

  // Hardcoded parameters for now
  static String host = "localhost";
  static int port = 5432;
  static String user = "linkbench";
  static String pass = "pw";
  static String linktable = "test_linktable";
  static String counttable = "test_counttable";
  static String nodetable = "test_nodetable";

  public static void fillPgsqlTestServerProps(Properties props) {
    props.setProperty(Config.LINKSTORE_CLASS, LinkStorePgsql.class.getName());
    props.setProperty(Config.NODESTORE_CLASS, LinkStorePgsql.class.getName());
    props.setProperty(LinkStorePgsql.CONFIG_HOST, host);
    props.setProperty(LinkStorePgsql.CONFIG_PORT, Integer.toString(port));
    props.setProperty(LinkStorePgsql.CONFIG_USER, user);
    props.setProperty(LinkStorePgsql.CONFIG_PASSWORD, pass);
    props.setProperty(Config.LINK_TABLE, linktable);
    props.setProperty(Config.COUNT_TABLE, counttable);
    props.setProperty(Config.NODE_TABLE, nodetable);
  }

  static Connection createConnection(String testDB)
     throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException, ReflectiveOperationException {
    Logger.getLogger().info("create connection");
    Class.forName("org.postgresql.Driver").getConstructor().newInstance();
    String jdbcUrl = "jdbc:postgresql://"+ host + ":" + port + "/linkbench";
    jdbcUrl += "?elideSetAutoCommits=true" +
               "&useLocalTransactionState=true" +
               "&allowMultiQueries=true" +
               "&useLocalSessionState=true" +
               "&useAffectedRows=true";

    return DriverManager.getConnection(jdbcUrl, PgsqlTestConfig.user, PgsqlTestConfig.pass);
  }

  static void createTestTables(Connection conn, String testDB) throws SQLException {
    Logger.getLogger().info("createTestTables");
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("DROP SCHEMA IF EXISTS " + testDB + " CASCADE");
    stmt.executeUpdate("CREATE SCHEMA " + testDB);

    stmt.executeUpdate(String.format(
	"CREATE TABLE %s.%s ( " +
        "  id1 bigint NOT NULL DEFAULT '0', " +
        "  id2 bigint NOT NULL DEFAULT '0', " +
        "  link_type bigint NOT NULL DEFAULT '0', " +
        "  visibility smallint NOT NULL DEFAULT '0', " +
        "  data bytea NOT NULL , " +
        "  time bigint NOT NULL DEFAULT '0', " +
        "  version int NOT NULL DEFAULT '0', " +
        "  PRIMARY KEY (link_type, id1,id2))",
        testDB, PgsqlTestConfig.linktable));

    stmt.executeUpdate(String.format(
        "CREATE INDEX id1_type on %s.%s ( " +
        "  id1,link_type,visibility,time,id2,version,data)",
        testDB, PgsqlTestConfig.linktable));

    stmt.executeUpdate(String.format(
        "CREATE TABLE %s.%s ( " +
        "  id bigint NOT NULL DEFAULT '0', " +
        "  link_type bigint NOT NULL DEFAULT '0', " +
        "  count bigint NOT NULL DEFAULT '0', " +
        "  time bigint NOT NULL DEFAULT '0', " +
        "  version bigint NOT NULL DEFAULT '0', " +
        "  PRIMARY KEY (id,link_type))",
        testDB, PgsqlTestConfig.counttable));

    stmt.executeUpdate(String.format(
        "CREATE TABLE %s.%s ( " +
        "  id BIGSERIAL NOT NULL, " +
        "  type int NOT NULL, " +
        "  version bigint NOT NULL, " +
        "  time int NOT NULL, " +
        "  data bytea NOT NULL, " +
        "  PRIMARY KEY(id))",
        testDB, PgsqlTestConfig.nodetable));

    stmt.close();
  }

  static void dropTestTables(Connection conn, String testDB) throws SQLException {
    Logger.getLogger().info("dropTestTables");
    Statement stmt = conn.createStatement();
    int rlink = stmt.executeUpdate(String.format("DROP TABLE IF EXISTS %s.%s",
                                   testDB, PgsqlTestConfig.linktable));
    int rcount = stmt.executeUpdate(String.format("DROP TABLE IF EXISTS %s.%s",
                                    testDB, PgsqlTestConfig.counttable));
    int rnode = stmt.executeUpdate(String.format("DROP TABLE IF EXISTS %s.%s",
                                   testDB, PgsqlTestConfig.nodetable));
    if (rlink != 0 || rcount != 0 || rnode != 0) {
      throw new IllegalStateException("dropTestTables failed with (link,count,node)=(" +
                                      rlink + "," + rcount + "," + rnode + ")");
    }
    stmt.close();
  }
}
