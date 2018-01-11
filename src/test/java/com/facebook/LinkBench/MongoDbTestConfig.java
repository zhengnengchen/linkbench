package com.facebook.LinkBench;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.Properties;

/**
 * Class containing hardcoded parameters and helper functions used to create
 * and connect to the unit test database for MongoDB.
 */
public class MongoDbTestConfig {

    // Hardcoded parameters for now
    static String host = "localhost";
    static int port = 27017;
    static String user = "linkbench";
    static String pass = "linkbench";
    static String linktable = "test_linktable";
    static String counttable = "test_counttable";
    static String nodetable = "test_nodetable";

    public static void fillMongoDbTestServerProps(Properties props) {
        props.setProperty(Config.LINKSTORE_CLASS, LinkStoreMongoDb.class.getName());
        props.setProperty(Config.NODESTORE_CLASS, LinkStoreMongoDb.class.getName());
        props.setProperty(LinkStoreMongoDb.CONFIG_HOST, host);
        props.setProperty(LinkStoreMongoDb.CONFIG_PORT, Integer.toString(port));
        props.setProperty(LinkStoreMongoDb.CONFIG_USER, user);
        props.setProperty(LinkStoreMongoDb.CONFIG_PASSWORD, pass);
        props.setProperty(Config.LINK_TABLE, linktable);
        props.setProperty(Config.COUNT_TABLE, counttable);
        props.setProperty(Config.NODE_TABLE, nodetable);
    }

    static MongoDatabase createConnection(String testDB)
        throws InstantiationException,
        IllegalAccessException, ClassNotFoundException {

        return new MongoClient(host, port).getDatabase(testDB);

    }


    static void createTestTables(MongoDatabase conn, String testDB) {

        conn.createCollection(MongoDbTestConfig.linktable);
        conn.createCollection(MongoDbTestConfig.nodetable);
        conn.createCollection(MongoDbTestConfig.counttable);

    }

    static void dropTestTables(MongoDatabase conn, String testDB) {

        conn.getCollection(MongoDbTestConfig.linktable).drop();
        conn.getCollection(MongoDbTestConfig.nodetable).drop();
        conn.getCollection(MongoDbTestConfig.counttable).drop();

    }
}
