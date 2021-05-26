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

CREATE FUNCTION griddb_create_or_replace_stub(func_type text, name_arg text, return_type regtype) RETURNS BOOL AS $$
DECLARE
  proname_raw text := split_part(name_arg, '(', 1);
  proname text := ltrim(rtrim(proname_raw));
BEGIN
  IF lower(func_type) = 'aggregation' OR lower(func_type) = 'aggregate' OR lower(func_type) = 'agg' OR lower(func_type) = 'a' THEN
    DECLARE
      proargs_raw text := right(name_arg, length(name_arg) - length(proname_raw));
      proargs text := ltrim(rtrim(proargs_raw));
      proargs_types text := right(left(proargs, length(proargs) - 1), length(proargs) - 2);
      aggproargs text := format('(%s, %s)', return_type, proargs_types);
    BEGIN
      BEGIN
        EXECUTE format('
          CREATE FUNCTION %s_sfunc%s RETURNS %s IMMUTABLE AS $inner$
          BEGIN
            RAISE EXCEPTION ''stub %s_sfunc%s is called'';
            RETURN NULL;
          END $inner$ LANGUAGE plpgsql;',
	  proname, aggproargs, return_type, proname, aggproargs);
      EXCEPTION
        WHEN duplicate_function THEN
          RAISE DEBUG 'stub function for aggregation already exists (ignored)';
      END;
      BEGIN
        EXECUTE format('
          CREATE AGGREGATE %s
          (
            sfunc = %s_sfunc,
            stype = %s
          );', name_arg, proname, return_type);
      EXCEPTION
        WHEN duplicate_function THEN
          RAISE DEBUG 'stub aggregation already exists (ignored)';
        WHEN others THEN
          RAISE EXCEPTION 'stub aggregation exception';
      END;
    END;
  ELSEIF lower(func_type) = 'function' OR lower(func_type) = 'func' OR lower(func_type) = 'f' THEN
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s RETURNS %s IMMUTABLE AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  ELSEIF lower(func_type) = 'time-series-function' OR lower(func_type) = 'ts_func' OR lower(func_type) = 'tsf' THEN
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s RETURNS %s STABLE AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  ELSE
    RAISE EXCEPTION 'not supported function type %', func_type;
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s_sfunc RETURNS %s AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  END IF;
  RETURN TRUE;
END
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'griddb_time_unit') THEN
      CREATE TYPE griddb_time_unit as enum ('YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'MILLISECOND');
    END IF;
END$$;

/* Time Operations */
SELECT griddb_create_or_replace_stub('f', 'to_timestamp_ms(bigint)', 'timestamp');
SELECT griddb_create_or_replace_stub('f', 'to_epoch_ms(timestamp)', 'bigint');
SELECT griddb_create_or_replace_stub('f', 'griddb_timestamp(text)', 'timestamp');
SELECT griddb_create_or_replace_stub('f', 'timestampadd(griddb_time_unit, timestamp, integer)', 'timestamp');
SELECT griddb_create_or_replace_stub('f', 'timestampdiff(griddb_time_unit, timestamp, timestamp)', 'double precision');

/* Array Operations */
SELECT griddb_create_or_replace_stub('f', 'array_length(anyarray)', 'integer');
SELECT griddb_create_or_replace_stub('f', 'element(integer, anyarray)', 'anyelement');

/* Time-series functions */
SELECT griddb_create_or_replace_stub('tsf', 'time_next(timestamp)', 'text');
SELECT griddb_create_or_replace_stub('tsf', 'time_next_only(timestamp)', 'text');
SELECT griddb_create_or_replace_stub('tsf', 'time_prev(timestamp)', 'text');
SELECT griddb_create_or_replace_stub('tsf', 'time_prev_only(timestamp)', 'text');
SELECT griddb_create_or_replace_stub('tsf', 'time_interpolated(anyelement, timestamp)', 'text');
SELECT griddb_create_or_replace_stub('tsf', 'max_rows(anyelement)', 'text');
SELECT griddb_create_or_replace_stub('tsf', 'min_rows(anyelement)', 'text');
SELECT griddb_create_or_replace_stub('tsf', 'time_sampling(timestamp,timestamp,integer,griddb_time_unit)', 'text');
SELECT griddb_create_or_replace_stub('tsf', 'time_sampling(anyelement,timestamp,timestamp,integer,griddb_time_unit)', 'text');

/* Aggregate function */
SELECT griddb_create_or_replace_stub('a', 'time_avg(anyelement)', 'double precision');