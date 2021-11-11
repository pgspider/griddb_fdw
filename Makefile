# contrib/griddb_fdw/Makefile

MODULE_big = griddb_fdw
OBJS = griddb_fdw.o option.o deparse.o connection.o compare.o store.o $(WIN32RES)
PGFILEDESC = "griddb_fdw - foreign data wrapper for GridDB"

GRIDDB_INCLUDE = griddb/client/c/include
GRIDDB_LIBRARY = griddb/bin

PG_CPPFLAGS = -I$(libpq_srcdir) -I$(GRIDDB_INCLUDE)
SHLIB_LINK = $(libpq) -L$(GRIDDB_LIBRARY) -lgridstore

EXTENSION = griddb_fdw
DATA = griddb_fdw--1.0.sql griddb_fdw--1.0--1.1.sql griddb_fdw--1.1.sql griddb_fdw--1.2.sql

REGRESS = griddb_fdw selectfunc griddb_fdw_data_type float4 float8 int4 int8 numeric join limit aggregates prepare select_having select insert update griddb_fdw_post

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/griddb_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifdef REGRESS_PREFIX
REGRESS_PREFIX_SUB = $(REGRESS_PREFIX)
else
REGRESS_PREFIX_SUB = $(VERSION)
endif

REGRESS := $(addprefix $(REGRESS_PREFIX_SUB)/,$(REGRESS))
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB))
