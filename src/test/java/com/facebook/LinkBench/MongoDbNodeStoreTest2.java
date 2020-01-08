package com.facebook.LinkBench;

import com.mongodb.client.MongoDatabase;

import java.io.IOException;
import java.util.Properties;


public class MongoDbNodeStoreTest2 extends NodeStoreTestBase {

    MongoDatabase conn;
    Properties currProps;

    @Override
    protected Properties basicProps() {
        Properties props = super.basicProps();
        MongoDbTestConfig.fillMongoDbTestServerProps(props);
        return props;
    }

    @Override
    protected void initNodeStore(Properties props) throws Exception, IOException {
        currProps = props;
        conn = MongoDbTestConfig.createConnection(testDB);
        MongoDbTestConfig.dropTestTables(conn, testDB);
        MongoDbTestConfig.createTestTables(conn, testDB);
    }

    @Override
    protected NodeStore getNodeStoreHandle(boolean initialize) throws Exception, IOException {
        DummyLinkStore result = new DummyLinkStore(new LinkStoreMongoDb());
        if (initialize) {
            result.initialize(currProps, Phase.REQUEST, 0);
        }
        return result;
    }

}

