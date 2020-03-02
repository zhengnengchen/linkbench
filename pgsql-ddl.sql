--create database for linkbench

drop database if exists linkdb0;
create database linkdb0 encoding='latin1' ;

--drop user linkbench to create new one
DROP USER  IF EXISTS linkbench;

--	You may want to set up a special database user account for benchmarking:
CREATE USER linkbench password 'linkbench';
-- Grant all privileges on linkdb0 to this user
GRANT ALL ON database linkdb0 TO linkbench;

--add Schema keep the same query style
DROP SCHEMA IF EXISTS linkdb0 CASCADE; 
CREATE SCHEMA linkdb0;

--conn postgresql linkbench/password

--FIXME:Need to make it partitioned by key id1 %16
CREATE TABLE linkdb0.linktable (
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
CREATE INDEX id1_type on linkdb0.linktable(id1,link_type,visibility,time,id2,version,data);

CREATE TABLE linkdb0.counttable (
		id numeric(20) NOT NULL DEFAULT '0',
		link_type numeric(20) NOT NULL DEFAULT '0',
		count int NOT NULL DEFAULT '0',
		time numeric(20) NOT NULL DEFAULT '0',
		version numeric(20) NOT NULL DEFAULT '0',
		PRIMARY KEY (id,link_type)
		);

CREATE TABLE linkdb0.nodetable (
		id BIGSERIAL NOT NULL,
		type int NOT NULL,
		version numeric NOT NULL,
		time int NOT NULL,
		data text NOT NULL,
		PRIMARY KEY(id)
		);

