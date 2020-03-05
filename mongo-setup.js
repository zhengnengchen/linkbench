//
// create the required databases and indexes
//
// use like:
// mongo mongo-setup.js # for single linkdb0 database
// mongo --eval 'var numdbs=10;' mongo-setup.js # for linkdb0 .. linkdb9 databases
//
if (typeof(numdbs) === 'undefined') {
    numdbs = 1;
}
for(var i =0; i < numdbs; i++) {
    db = db.getSiblingDB('linkdb' + i);
    db.createCollection("linktable");
    db.createCollection("nodetable");
    db.createCollection("counttable");

    db.linktable.createIndex({id1: 1, link_type: 1, time: 1, visibility: 1});
    db.counttable.createIndex({id1: 1, link_type: 1})
}
