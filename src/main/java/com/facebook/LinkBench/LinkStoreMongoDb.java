/**
 * LinkStore implementation for MongoDB, using the 'doTxn' command to do transactions.
 */

package com.facebook.LinkBench;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.*;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

import org.bson.*;
import org.bson.Document;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.Binary;


public class LinkStoreMongoDb extends GraphStore {

    /* MongoDB database server configuration keys */
    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_USER = "user";
    public static final String CONFIG_PASSWORD = "password";

    public static final int DEFAULT_BULKINSERT_SIZE = 1024;

    private static final boolean INTERNAL_TESTING = false;

    // In MongoDB, these will be "collections", which are the analog of SQL "tables". We call them
    // "tables" here to keep the naming convention consistent across different database benchmarks.
    String linktable;
    String counttable;
    String nodetable;

    String host;
    String user;
    String pwd;
    int port;

    Level debuglevel;

    private MongoClient mongoClient;

    // A monotonically increasing counter used to assign unique graph node ids. We make it atomic in case it is
    // accessed from multiple threads. We use this since there is no default way to have an 'auto-incrementing' field
    // in MongoDB documents.
    private AtomicLong nodeIdGen = new AtomicLong(0);

    private Phase phase;

    int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;

    private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

    public LinkStoreMongoDb() {
        super();
    }

    public LinkStoreMongoDb(Properties props) throws IOException, Exception {
        super();
        initialize(props, Phase.LOAD, 0);
    }

    public void initialize(Properties props, Phase currentPhase, int threadId) throws IOException, Exception {

        // Retrieve names of database tables that will be used.
        counttable = ConfigUtil.getPropertyRequired(props, Config.COUNT_TABLE);
        if (counttable.equals("")) {
            String msg = "Error! " + Config.COUNT_TABLE + " is empty!"
                + "Please check configuration file.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        nodetable = props.getProperty(Config.NODE_TABLE);
        if (nodetable.equals("")) {
            // For now, don't assume that nodetable is provided
            String msg = "Error! " + Config.NODE_TABLE + " is empty!"
                + "Please check configuration file.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        linktable = props.getProperty(Config.LINK_TABLE);
        if (linktable.equals("")) {
            // For now, don't assume that linktable is provided
            String msg = "Error! " + Config.LINK_TABLE + " is empty!"
                + "Please check configuration file.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
        user = ConfigUtil.getPropertyRequired(props, CONFIG_USER);
        pwd = ConfigUtil.getPropertyRequired(props, CONFIG_PASSWORD);
        String portProp = props.getProperty(CONFIG_PORT);

        if (portProp == null || portProp.equals("")) {
            port = 27017; //use default port
        } else {
            port = Integer.parseInt(portProp);
        }

        debuglevel = ConfigUtil.getDebugLevel(props);
        phase = currentPhase;

        // Connect to database.
        try {
            openConnection();
        } catch (Exception e) {
            logger.error("error connecting to database:", e);
            throw e;
        }
    }

    private void openConnection() throws Exception {
        // Open client connection to MongoDB server.
        mongoClient = new MongoClient(host, port);

        // Run a basic status command to make sure the connection is created and working properly.
        MongoDatabase adminDb = mongoClient.getDatabase("admin");
        adminDb.runCommand(new Document("serverStatus", 1));
    }

    @Override
    public void close() {
        mongoClient.close();
    }

    @Override
    public void clearErrors(final int threadID) {
        logger.info("Closing and re-opening MongoDB client connection in threadID " + threadID);

        mongoClient.close();

        try {
            openConnection();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    /**
     * Set of all error codes that indicate a transient MongoDB error
     * that should be handled by retrying.
     */
    private static final HashSet<Integer> retryMongoCodes = populateMongoRetryCodes();

    /**
     *  Populate retryMongoCodes.
     *  These error codes are defined in MongoDB source code:
     *  https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
     *
     *  TODO: Review error codes when full transaction support is implemented in MongoDB.
     */
    private static HashSet<Integer> populateMongoRetryCodes() {
        HashSet<Integer> states = new HashSet<>();
        states.add(24);     // LockTimeout
        states.add(46);     // LockBusy
        states.add(112);    // WriteConflict
        states.add(117);    // ConflictingOperationInProgress
        states.add(208);    // TooManyLocks
        states.add(225);    // TransactionTooOld
        states.add(226);    // AtomicityFailure
        return states;
    }

    /**
     * Create a BSON precondition object, suitable for use in a transaction statement. A precondition
     * consists of a query, which should uniquely identify a document, if such a document exists, and a match statement,
     * which defines whether or not the precondition passes. That is, the precondition is satisfied iff the document
     * returned by the query satisfies the match statement.
     *
     * @param query
     * @param match
     * @return the precondition BSON object.
     */
    private BsonDocument createPrecondition(BsonDocument query, BsonDocument match, MongoNamespace ns) {
        return new BsonDocument().append("q", query)
            .append("res", match)
            .append("ns", new BsonString(ns.toString()));
    }

    /**
     * Predicate that determines whether a 'doTxn' error response is due to a failed pre-condition.
     */
    private boolean preConditionFailedError(String errMsg, int errCode) {
        return errMsg.contains("preCondition failed") && errCode == 2;
    }

    /**
     * Predicate that determines whether a MongoDB transaction error code is considered retryable.
     */
    private boolean isRetryableError(int errCode) {
        return retryMongoCodes.contains(errCode);
    }

    /**
     * Execute a list of CRUD operations as a single atomic unit, only if all the given pre-conditions are satisfied.
     * Until MongoDB supports full read/write transactions, this function utilizes the 'doTxn' command along with its
     * provided "precondition" functionality to emulate this.
     *
     * A given transaction might fail for a number of reasons. If it fails due to an unsatisfied precondition, this
     * function will return false. In this error case, a caller can be guaranteed that the
     * transaction did not succeed, and no changes were made to the database. If the transaction fails for some other
     * reason related to the driver or the server, this function will throw the corresponding exception, for the caller
     * to handle. If the transaction fails due to a "retriable" error, the transaction will be retried indefinitely.
     *
     * NOTE 1: The performance of precondition checking will depend on existence of appropriate indexes.
     * NOTE 2: For any 'update' operations in a transaction, the query (i.e. the 'o2' field) must identify a
     * document by its '_id' field. This is because the 'doTxn' command does not presently support applying an
     * update that does not uniquely specify a document by its '_id'.
     *
     * @param operations list of operations to execute.
     * @param preConditions a set of preconditions that must be true in order for the transaction to execute.
     * @return If the transaction succeeds, returns true. If it fails due to a pre-condition check, it will return false.
     * @throws Exception if transaction fails for some reason other than an unsatisfied pre-condition.
     */
    private boolean doTxnWithPreconditions(
        MongoDatabase database,
        List<BsonDocument> operations,
        List<BsonDocument> preConditions) throws Exception
    {
        BsonDocument cmdObj = new BsonDocument();
        cmdObj.append("doTxn", new BsonArray(operations));
        cmdObj.append("preCondition", new BsonArray(preConditions));

        while (true) {
            try {
                Document res = database.runCommand(cmdObj);
                assert (res.getDouble("ok") == 1.0);
                return true;
            } catch (MongoCommandException e) {
                // If we failed due to an unsatisfied pre-condition, return false.
                if (preConditionFailedError(e.getMessage(), e.getCode())) {
                    return false;
                }
                // If we failed due to a non-retryable error, throw the exception.
                if (!isRetryableError(e.getCode())) {
                    throw e;
                }
            }
        }
    }

    /**
     * Adds a given link to the database and updates counts atomically, only if the link does not already exist.
     * @param dbid
     * @param link
     * @param noinverse
     * @return If the link already exists, does not modify the database and returns false. If the link was added,
     * returns true. If the operation fails for a reason other than the link not existing, will throw an Exception.
     * @throws Exception
     */
    private boolean addLinkIfNotExists(final String dbid, final Link link, final boolean noinverse) throws Exception {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> linkCollection = database.getCollection(linktable);
        MongoCollection<Document> countCollection = database.getCollection(counttable);

        // We build the INSERT_LINK operation and the UPDATE_COUNT operation as part of the
        // same transaction. We aren't able to do transactional updates using 'doTxn' that target a
        // document by fields other than '_id', so that is why we are using the concatenated unique fields of the
        // link object (id1, link_type, id2) as its '_id'.

        // INSERT_LINK OP.
        BsonDocument linkId = linkBsonId(link);
        BsonDocument linkBson = linkToBson(link).append("_id", linkId);
        BsonDocument insertLinkOp = createInsertOp(linkBson, linkCollection.getNamespace());

        // If the new link is hidden, we don't count it.
        long updateCount = link.visibility == VISIBILITY_DEFAULT ? 1 : 0;

        BsonDocument countId = countBsonId(link.id1, link.link_type);

        // UPDATE_COUNT OP.
        BsonDocument updateQ = eq("_id", countId)
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument countUpdate = combine(
            set("_id", countId),
            set("id1", new BsonInt64(link.id1)),
            set("link_type", new BsonInt64(link.link_type)),
            set("time", new BsonInt64(link.time)),
            inc("count", updateCount),
            inc("version", 1))
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument updateCountOp = createUpdateOp(updateQ, countUpdate, countCollection.getNamespace());

        // Precondition: link mustn't already exist.
        BsonDocument preConditionQ = eq("_id", linkId)
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        // Precondition match is satisfied only if there is no document returned by the query.
        BsonDocument preConditionMatch = exists("id1", false)
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument preCondition = createPrecondition(preConditionQ, preConditionMatch, linkCollection.getNamespace());
        List<BsonDocument> opsToApply = Arrays.asList(insertLinkOp, updateCountOp);

        return doTxnWithPreconditions(database, opsToApply, Collections.singletonList(preCondition));
    }

    /**
     * Updates a given link only if the link exists and if the link's current visibility matches the given visibility level.
     * @param dbid
     * @param link
     * @param noinverse
     * @return If the link exists with the correct visibility, updates the link and returns true. If the link exists
     * without the correct visibility, does nothing and returns false.
     * @throws Exception
     */
    private boolean updateLinkIfVisibility(
        final String dbid,
        final Link link,
        final boolean noinverse,
        byte currentVisibility) throws Exception
    {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> linkCollection = database.getCollection(linktable);
        MongoCollection<Document> countCollection = database.getCollection(counttable);

        // UPDATE_LINK OP.

        BsonDocument linkId = linkBsonId(link);
        BsonDocument updateLinkQ = eq("_id", linkId)
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument linkUpdate = combine(
            set("visibility", Byte.toString(link.visibility)),
            set("time", link.time),
            set("data", link.data),
            set("version", link.version))
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument updateLinkOp = createUpdateOp(updateLinkQ, linkUpdate, linkCollection.getNamespace());

        // UPDATE_COUNT OP.

        // If we update the link and it was previously hidden, we increment the count. In the opposite case, we
        // decrement the count. If visibility doesn't change, don't increment or decrement the count.
        int updateCount;
        if (currentVisibility == link.visibility) {
            updateCount = 0;
        } else {
            // 'link.visibility' will be the new visibility of the link if the update transaction succeeds.
            updateCount = link.visibility == VISIBILITY_DEFAULT ? 1 : -1;
        }

        BsonDocument countId = countBsonId(link.id1, link.link_type);
        BsonDocument updateQ = eq("_id", countId)
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument countUpdate = combine(
            set("_id", countId),
            set("id1", new BsonInt64(link.id1)),
            set("link_type", new BsonInt64(link.link_type)),
            set("time", new BsonInt64(link.time)),
            inc("count", updateCount),
            inc("version", 1))
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument updateCountOp = createUpdateOp(updateQ, countUpdate, countCollection.getNamespace());

        // Precondition: link must exist and have the right visibility.
        BsonDocument preConditionQ = eq("_id", linkId)
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        byte[] currVisibility = { currentVisibility };
        BsonDocument preConditionMatch =
            and(
                eq("id1", link.id1),
                eq("link_type", link.link_type),
                eq("id2", link.id2),
                eq("visibility", Byte.toString(currentVisibility)))
                .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument preConditionLinkExistsWithVisibility =
            createPrecondition(preConditionQ, preConditionMatch, linkCollection.getNamespace());
        List<BsonDocument> opsToApply = Arrays.asList(updateLinkOp, updateCountOp);

        return doTxnWithPreconditions(
            database,
            opsToApply,
            Collections.singletonList(preConditionLinkExistsWithVisibility));
    }

    @Override
    public boolean addLink(final String dbid, final Link a, final boolean noinverse) throws Exception {
        logger.debug("addLink " + a.id1 +
            "." + a.id2 +
            "." + a.link_type);

        // Until MongoDB fully supports read/write transactions, we don't have any way to make a decision inside of a transaction
        // based on the result of an earlier operation inside that transaction. For example, if we try to add a visible
        // link and it already exists with default visibility, we don't need to update the count table. On the other hand,
        // if a new link was added, then we need to update the counts. To circumvent this, we separate the ADD_LINK operation
        // into distinct cases, that only succeed (atomically) if the proper preconditions are met. We can continually
        // retry the below sequence of operations until success. If any operation fails due to a pre-condition check, we know
        // that the database was not modified, so it is safe to try again.

        while (true) {
            // Try to add the link if it doesn't already exist. If this succeeds, we have nothing further to do. If it
            // returns false, then that implies the link already exists, so we need to attempt a link update.
            if (addLinkIfNotExists(dbid, a, noinverse)) {
                return true;
            }

            // If the link already exists, try to update it only if it is currently visible.
            if (updateLinkIfVisibility(dbid, a, noinverse, VISIBILITY_DEFAULT)) {
                return false;
            }

            // If the link already exists, try to update it only if it is currently hidden.
            if (updateLinkIfVisibility(dbid, a, noinverse, VISIBILITY_HIDDEN)) {
                return false;
            }
        }
    }

    /**
     * Deletes a link specified by the given parameters only if it exists in the database and it's current visibility matches
     * 'visibility'. If expunge==true, the link is always physically deleted from the database. Otherwise, it's
     * visibility is changed to HIDDEN. If a visible link is deleted then the associated counts must be updated.
     * @param dbid
     * @param id1
     * @param link_type
     * @param id2
     * @param noinverse
     * @param expunge
     * @param visibility the required visibility of the existing link.
     * @return
     * @throws Exception
     */
    private boolean deleteLinkIfExistsWithVisibility(
        final String dbid,
        final long id1,
        final long link_type,
        final long id2,
        final boolean noinverse,
        final boolean expunge,
        byte visibility) throws Exception
    {

        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> linkCollection = database.getCollection(linktable);
        MongoCollection<Document> countCollection = database.getCollection(counttable);

        // DELETE_LINK operation. Only delete the link if link is found by (id1, id2, link_type) match.
        BsonDocument linkId = linkBsonId(id1, link_type, id2);
        BsonDocument findLinkPred =
            eq("_id", linkId).toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument deleteLinkOp;

        // If expunge==true, then we physically remove the link from the database, no matter what.
        if (expunge) {
            deleteLinkOp = createDeleteOp(findLinkPred, linkCollection.getNamespace());
        }
        // If expunge==false, then we just hide the link.
        else {
            BsonDocument update = set("visibility", Byte.toString(VISIBILITY_HIDDEN))
                .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
            deleteLinkOp = createUpdateOp(findLinkPred, update, linkCollection.getNamespace());
        }

        ArrayList<BsonDocument> opsToApply = new ArrayList<>();
        opsToApply.add(deleteLinkOp);

        // UPDATE_COUNT operation. Only update the counts if it would have an effect i.e. if the link exists and is
        // visible.
        int countInc = visibility == VISIBILITY_DEFAULT ? -1 : 0;
        if (countInc != 0) {
            BsonDocument countId = countBsonId(id1, link_type);
            BsonDocument updateQ = eq("_id", countId)
                .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

            long currentTime = (new Date()).getTime();
            BsonDocument countUpdate = combine(
                set("time", new BsonInt64(currentTime)),
                inc("count", -1),
                inc("version", 1))
                .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

            BsonDocument updateCountOp = createUpdateOp(updateQ, countUpdate, countCollection.getNamespace());

            // Include the op to update counts.
            opsToApply.add(updateCountOp);
        }

        // Precondition: link must exist and visibility must be correct.
        BsonDocument preConditionQ = eq("_id", linkId)
            .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
        BsonDocument preConditionMatch =
            and(
                eq("id1", id1),
                eq("link_type", link_type),
                eq("id2", id2),
                eq("visibility", Byte.toString(visibility)))
                .toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        BsonDocument preCondition = createPrecondition(preConditionQ, preConditionMatch, linkCollection.getNamespace());
        return doTxnWithPreconditions(database, opsToApply, Collections.singletonList(preCondition));

    }

    @Override
    public boolean deleteLink(
        final String dbid,
        final long id1,
        final long link_type,
        final long id2,
        final boolean noinverse,
        final boolean expunge)
        throws Exception
    {

        // CASE 1: Link exists and has VISIBILITY_DEFAULT.
        if (deleteLinkIfExistsWithVisibility(dbid, id1, link_type, id2, noinverse, expunge, VISIBILITY_DEFAULT)) {
            return true;
        }

        // CASE 2: Link exists and has VISIBILITY_HIDDEN.
        if (deleteLinkIfExistsWithVisibility(dbid, id1, link_type, id2, noinverse, expunge, VISIBILITY_HIDDEN)) {
            return true;
        }

        // CASE 3: Link does not exist. Do nothing.
        return true;

    }

    @Override
    public boolean updateLink(final String dbid, final Link a, final boolean noinverse) throws Exception {
        return !addLink(dbid, a, noinverse);
    }

    /**
     * Given a Link object, or the unique fields of a link, create a BSON object suitable for use as that link's '_id'
     * field.
     */
    private BsonDocument linkBsonId(Link link) {
        return linkBsonId(link.id1, link.link_type, link.id2);
    }

    private BsonDocument linkBsonId(long id1, long link_type, long id2) {
        return new BsonDocument()
            .append("id1", new BsonInt64(id1))
            .append("link_type", new BsonInt64(link_type))
            .append("id2", new BsonInt64(id2));
    }

    /**
     * Given a Count object, or the unique fields of a count, create a BSON object suitable for use as that count's '_id'
     * field.
     */
    private BsonDocument countBsonId(long id1, long link_type) {
        return new BsonDocument()
            .append("id1", new BsonInt64(id1))
            .append("link_type", new BsonInt64(link_type));
    }

    private BsonDocument countBsonId(LinkCount count) {
        return countBsonId(count.id1, count.link_type);
    }

    /**
     * Given a Link object, convert it to a BSON representation. Returns an object without an '_id' field.
     */
    private BsonDocument linkToBson(Link link) {
        BsonDocument linkObj = new BsonDocument();
        linkObj
            .append("id1", new BsonInt64(link.id1))
            .append("link_type", new BsonInt64(link.link_type))
            .append("id2", new BsonInt64(link.id2))
            // Serialize 'visibility' byte as a String.
            .append("visibility", new BsonString(Byte.toString(link.visibility)))
            .append("version", new BsonInt32(link.version))
            .append("time", new BsonInt64(link.time))
            .append("data", new BsonBinary(link.data));

        return linkObj;
    }

    /**
     * Given a document representing a Link, parse it into a Link object and return it.
     */
    private Link linkFromBson(Document linkDoc) {

        // Parse the retrieved link object.
        Link outLink = new Link(
            linkDoc.getLong("id1"),
            linkDoc.getLong("link_type"),
            linkDoc.getLong("id2"),
            Byte.parseByte(linkDoc.getString("visibility")),
            ((Binary) linkDoc.get("data")).getData(),
            linkDoc.getInteger("version"),
            linkDoc.getLong("time"));

        return outLink;
    }

    /**
     * Helper methods to create operation objects that can be used inside of a transaction statement.
     */
    private BsonDocument createDeleteOp(BsonDocument deleteQ, MongoNamespace ns) {
        return new BsonDocument().append("op", new BsonString("d"))
            .append("o", deleteQ)
            .append("ns", new BsonString(ns.toString()));
    }

    private BsonDocument createInsertOp(BsonDocument docToInsert, MongoNamespace ns) {
        return new BsonDocument().append("op", new BsonString("i"))
            .append("o", docToInsert)
            .append("ns", new BsonString(ns.toString()));
    }

    private BsonDocument createUpdateOp(BsonDocument updateQuery, BsonDocument update, MongoNamespace ns) {
        return new BsonDocument()
            .append("op", new BsonString("u"))
            .append("o", update)
            .append("o2", updateQuery)
            .append("b", new BsonBoolean(true)) // Enable upsert.
            .append("ns", new BsonString(ns.toString()));
    }

    @Override
    public Link getLink(final String dbid, final long id1, final long link_type, final long id2) throws Exception {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> linkCollection = database.getCollection(linktable);

        BsonDocument linkId = linkBsonId(id1, link_type, id2);
        Bson pred = eq("_id", linkId);

        MongoCursor<Document> cursor = linkCollection.find(pred).iterator();

        // Link not found.
        if (!cursor.hasNext()) {
            return null;
        }

        Document linkDoc = cursor.next();

        // Make sure the link type is correct.
        if (linkDoc.getLong("link_type") != link_type) {
            return null;
        }

        // Parse the retrieved link object and return it.
        // TODO: Consider implementing a custom codec for better performance.
        return linkFromBson(linkDoc);
    }

    @Override
    public Link[] getLinkList(final String dbid, final long id1, final long link_type) throws Exception {
        return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
    }

    @Override
    public Link[] getLinkList(
        final String dbid,
        final long id1,
        final long link_type,
        final long minTimestamp,
        final long maxTimestamp,
        final int offset,
        final int limit)
        throws Exception
    {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> linkCollection = database.getCollection(linktable);

        // Find the links, limited by min/max timestamp and sorted by timestamp (descending).
        Bson pred = and(
            eq("id1", id1),
            eq("link_type", link_type),
            gte("time", minTimestamp),
            lte("time", maxTimestamp),
            eq("visibility", new BsonString(Byte.toString(VISIBILITY_DEFAULT))));

        MongoCursor<Document> cursor = linkCollection
            .find(pred)
            .sort(new Document("time", -1))
            .skip(offset)
            .limit(limit).iterator();

        List<Link> links = new ArrayList<>();
        while (cursor.hasNext()) {
            Document linkDoc = cursor.next();
            links.add(linkFromBson(linkDoc));
        }

        // No links found.
        if (links.size() == 0) {
            return null;
        }

        // Return list of links as array.
        Link[] linkArr = new Link[links.size()];
        linkArr = links.toArray(linkArr);
        return linkArr;
    }

    /**
     * Internal method: add links without updating the count.
     * @param dbid
     * @param links the list of links to add
     * @return the number of links added to the database.
     * @throws Exception
     */
    private int addLinksNoCount(String dbid, List<Link> links) throws Exception {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> linkCollection = database.getCollection(linktable);
        List<BsonDocument> insertOps = new ArrayList<>();

        // Create a list of link insert operations suitable for the 'doTxn' command.
        for (Link link : links) {

            // Concatenate the unique fields of the link for its '_id', so we can query for it in transactional
            // 'update' operations.
            BsonDocument docToInsert = linkToBson(link).append("_id", linkBsonId(link));
            BsonDocument insertOp = createInsertOp(docToInsert, linkCollection.getNamespace());
            insertOps.add(insertOp);
        }

        // Run the 'doTxn' command.
        BsonDocument cmdObj = new BsonDocument();
        cmdObj.append("doTxn", new BsonArray(insertOps));
        cmdObj.append("allowAtomic", new BsonBoolean(true));
        Document res = database.runCommand(cmdObj);

        // Make sure the command succeeded and that all nodes were inserted.
        if (res.getDouble("ok") != 1.0) {
            throw new Exception("'doTxn' command failed.");
        }

        int nApplied = res.getInteger("applied");
        if (nApplied != links.size()) {
            throw new Exception("'doTxn' failed to bulk insert all nodes properly.");
        }

        return nApplied;
    }

    @Override
    public int bulkLoadBatchSize() {
        return bulkInsertSize;
    }

    public void addBulkLinks(String dbid, List<Link> a, boolean noinverse) throws Exception {
        int nAdded = addLinksNoCount(dbid, a);
        logger.trace("Added n=" + nAdded + " links");
    }

    @Override
    public void addBulkCounts(String dbid, List<LinkCount> counts)
        throws Exception
    {

        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> countCollection = database.getCollection(counttable);

        List<BsonDocument> insertOps = new ArrayList<>();
        for (LinkCount count : counts) {

            BsonDocument countId = countBsonId(count);
            BsonDocument insertDoc = new BsonDocument();
            insertDoc
                .append("_id", countId)
                .append("id1", new BsonInt64(count.id1))
                .append("link_type", new BsonInt64(count.link_type))
                .append("version", new BsonInt64(count.version))
                .append("time", new BsonInt64(count.time))
                .append("count", new BsonInt64(count.count));

            BsonDocument insertOp = new BsonDocument();
            insertOp
                .append("op", new BsonString("i"))
                .append("o", insertDoc)
                .append("ns", new BsonString(countCollection.getNamespace().toString()));

            insertOps.add(insertOp);
        }

        // Run the 'doTxn' command.
        BsonDocument cmdObj = new BsonDocument();
        cmdObj.append("doTxn", new BsonArray(insertOps));
        database.runCommand(cmdObj);
    }

    @Override
    public long countLinks(final String dbid, final long id1, final long link_type) throws Exception {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> countCollection = database.getCollection(counttable);

        // Find the right count entry.
        Bson pred = and(
            eq("id1", id1),
            eq("link_type", link_type));

        MongoCursor<Document> cursor = countCollection.find(pred).iterator();
        if (!cursor.hasNext()) {
            // No count entry exists.
            return 0;
        } else {
            // Return the count.
            return cursor.next().getLong("count");
        }

    }

    @Override
    public void resetNodeStore(final String dbid, final long startID) throws Exception {
        // Drop and re-create the node store collection to clear out all objects, and then reset the node id counter.
        MongoDatabase database = mongoClient.getDatabase(dbid);
        database.getCollection(nodetable).drop();
        database.createCollection(nodetable);
        nodeIdGen = new AtomicLong(startID);
    }

    @Override
    public long addNode(final String dbid, final Node node) throws Exception {
        long[] nodeIds = bulkAddNodes(dbid, Collections.singletonList(node));
        return nodeIds[0];
    }

    @Override
    public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {

        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> nodeCollection = database.getCollection(nodetable);
        List<BsonDocument> insertOps = new ArrayList<>();

        // Create a list of node insert operations suitable for the 'doTxn' command. We atomically pre-allocate a range
        // of node ids for the nodes we are going to insert. If the bulk node insertion fails, then these node ids will be
        // "missed" i.e. they will never be used again. We consider this acceptable, as long as, globally, node ids
        // always increase and no node id is ever assigned twice. This type of issue is similarly discussed in relation
        // to the MySQL "auto-increment" feature: https://dev.mysql.com/doc/refman/5.7/en/innodb-auto-increment-handling.html
        long nodeId = nodeIdGen.getAndAdd(nodes.size());
        long[] assignedNodeIds = new long[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            assignedNodeIds[i] = nodeId;
            BsonDocument docToInsert = new BsonDocument();
            docToInsert
                .append("_id", new BsonInt64(nodeId))
                .append("type", new BsonInt32(node.type))
                .append("version", new BsonInt64(node.version))
                .append("time", new BsonInt32(node.time))
                .append("data", new BsonBinary(node.data));

            BsonDocument insertOp = createInsertOp(docToInsert, nodeCollection.getNamespace());
            insertOps.add(insertOp);
            nodeId += 1;
        }

        // Run the 'doTxn' command.
        BsonDocument cmdObj = new BsonDocument();
        cmdObj.append("doTxn", new BsonArray(insertOps));
        cmdObj.append("allowAtomic", new BsonBoolean(true));
        Document res = database.runCommand(cmdObj);

        // Make sure the command succeeded and that all nodes were inserted.
        if (res.getDouble("ok") != 1.0) {
            throw new Exception("'doTxn' command failed while trying to insert node ids " +
                Long.toString(nodeId) +
                "-" +
                Long.toString(nodeId + nodes.size() - 1));
        }
        if (res.getInteger("applied") != nodes.size()) {
            throw new Exception("'doTxn' failed to bulk insert all nodes properly.");
        }

        return assignedNodeIds;

    }

    @Override
    public Node getNode(final String dbid, final int type, final long id) throws Exception {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        // Lookup the node by id and type.
        Bson query = and(eq("_id", id), eq("type", type));
        MongoCursor<Document> cursor = nodeCollection.find(query).iterator();

        // Node not found.
        if (!cursor.hasNext()) {
            return null;
        }

        // Fetch and parse the retrieved node object.
        // TODO: Consider implementing a custom codec for better performance.
        Document nodeDoc = cursor.next();
        Binary bindata = (Binary) nodeDoc.get("data");
        byte data[] = bindata.getData();
        Node outNode = new Node(
            nodeDoc.getLong("_id"),
            nodeDoc.getInteger("type"),
            nodeDoc.getLong("version"),
            nodeDoc.getInteger("time"),
            data);

        return outNode;
    }

    @Override
    public boolean updateNode(final String dbid, final Node node) throws Exception {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        Bson nodeUpdate = combine(
            set("type", new BsonInt32(node.type)),
            set("version", new BsonInt64(node.version)),
            set("time", new BsonInt32(node.time)),
            set("data", new BsonBinary(node.data)));

        // Update the node with the specified id.
        UpdateResult res = nodeCollection.updateOne(eq("_id", node.id), nodeUpdate);

        // Node not found.
        if (res.getMatchedCount() == 0) {
            return false;
        }

        // Node was updated.
        if (res.getModifiedCount() == 1) {
            return true;
        }

        // Consider this an error.
        return false;
    }

    @Override
    public boolean deleteNode(final String dbid, final int type, final long id) throws Exception {
        MongoDatabase database = mongoClient.getDatabase(dbid);
        MongoCollection<Document> nodeCollection = database.getCollection(nodetable);

        // Only delete the node if the 'id' and 'type' match.
        Bson pred = and(eq("_id", id), eq("type", type));
        DeleteResult res = nodeCollection.deleteOne(pred);

        // Return true if the node was deleted, else return false.
        return (res.getDeletedCount() == 1);

    }
}
