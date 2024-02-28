# contrib/percona_telemetry/Makefile

MODULE_big = percona_telemetry
OBJS = \
	$(WIN32RES) \
	percona_telemetry.o

EXTENSION = percona_telemetry
DATA = percona_telemetry--1.0.sql

PGFILEDESC = "percona_telemetry - extension for Percona telemetry data collection."

REGRESS = basic

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/percona_telemetry
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
