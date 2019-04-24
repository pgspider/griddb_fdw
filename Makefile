# contrib/griddb_fdw/Makefile

MODULE_big = griddb_fdw
OBJS = griddb_fdw.o option.o deparse.o connection.o compare.o store.o $(WIN32RES)
PGFILEDESC = "griddb_fdw - foreign data wrapper for GridDB"

GRIDDB_INCLUDE = griddb/client/c/include
GRIDDB_LIBRARY = griddb/bin
GRIDDB_INIT = griddb_init

# Make for initializer
all:
	$(CC) make_check_initializer/griddb_init.c -o $(GRIDDB_INIT) -I$(GRIDDB_INCLUDE) -L$(GRIDDB_LIBRARY) -lgridstore
	./$(GRIDDB_INIT) 239.0.0.1 31999 ktymCluster admin testadmin

PG_CPPFLAGS = -I$(libpq_srcdir) -I$(GRIDDB_INCLUDE)
SHLIB_LINK = $(libpq) -L$(GRIDDB_LIBRARY) -lgridstore

EXTENSION = griddb_fdw
DATA = griddb_fdw--1.0.sql

EXTRA_CLEAN = ./$(GRIDDB_INIT)

REGRESS = griddb_fdw griddb_fdw_data_type

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
