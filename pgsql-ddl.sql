--create database for linkbench

drop database if exists linkdb;
create database linkdb encoding='latin1' ;

--drop user linkbench to create new one
DROP USER  IF EXISTS linkbench;

--	You may want to set up a special database user account for benchmarking:
CREATE USER linkbench password 'linkbench';
-- Grant all privileges on linkdb to this user
GRANT ALL ON database linkdb TO linkbench;

--add Schema keep the same query style
DROP SCHEMA IF EXISTS linkdb CASCADE; 
CREATE SCHEMA linkdb;

--conn postgresql linkbench/password

--FIXME:Need to make it partitioned by key id1 %16
CREATE TABLE linkdb.linktable (
		id1 numeric(20) NOT NULL DEFAULT '0',
		id2 numeric(20) NOT NULL DEFAULT '0',
		link_type numeric(20) NOT NULL DEFAULT '0',
		visibility smallint NOT NULL DEFAULT '0',
		data varchar(255) NOT NULL DEFAULT '',
		time numeric(20) NOT NULL DEFAULT '0',
		version bigint NOT NULL DEFAULT '0',
		PRIMARY KEY (link_type, id1,id2)
		);

-- this is index for linktable
CREATE INDEX id1_type on linkdb.linktable(id1,link_type,visibility,time,id2,version,data);

CREATE TABLE linkdb.counttable (
		id numeric(20) NOT NULL DEFAULT '0',
		link_type numeric(20) NOT NULL DEFAULT '0',
		count int NOT NULL DEFAULT '0',
		time numeric(20) NOT NULL DEFAULT '0',
		version numeric(20) NOT NULL DEFAULT '0',
		PRIMARY KEY (id,link_type)
		);

CREATE TABLE linkdb.nodetable (
		id BIGSERIAL NOT NULL,
		type int NOT NULL,
		version numeric NOT NULL,
		time int NOT NULL,
		data text NOT NULL,
		PRIMARY KEY(id)
		);

