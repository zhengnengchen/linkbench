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
        MongoDbTestConfig2.fillMongoDbTestServerProps(props);
        return props;
    }

    @Override
    protected void initNodeStore(Properties props) throws Exception, IOException {
        currProps = props;
        conn = MongoDbTestConfig2.createConnection(testDB);
        MongoDbTestConfig2.dropTestTables(conn, testDB);
        MongoDbTestConfig2.createTestTables(conn, testDB);
    }

    @Override
    protected NodeStore getNodeStoreHandle(boolean initialize) throws Exception, IOException {
        DummyLinkStore result = new DummyLinkStore(new LinkStoreMongoDb2());
        if (initialize) {
            result.initialize(currProps, Phase.REQUEST, 0);
        }
        return result;
    }

}

