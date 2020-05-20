/* contrib/griddb_fdw/griddb_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION griddb_fdw" to load this file. \quit

CREATE FUNCTION griddb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION griddb_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER griddb_fdw
  HANDLER griddb_fdw_handler
  VALIDATOR griddb_fdw_validator;
