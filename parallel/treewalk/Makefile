CC		=	gcc
CFLAGS		=	-Wall -D_GNU_SOURCE -g
CLIBS		=   -lconfig

MPICC		=	mpicc
MPICFLAGS	=	$(MPI_COMPILE_FLAGS) -g
MPICLIBS	=	$(MPI_LD_FLAGS)

PGLIBS		=	-L`pg_config --libdir` -lpq
PGINCLUDE	=	-I`pg_config --includedir`

UTILOBJS	=	../common/utils.o ../../src/config.o ../../src/log.o
OBJECTS		=	pstat.o


all:	pstat

pstat:	$(OBJECTS) $(UTILOBJS)
	$(MPICC) $(MPICLIBS) $(PGLIBS) $(PGINCLUDE) \
		$(OBJECTS) $(UTILOBJS) $(CLIBS) -o pstat

pstat.o:	../common/utils.o pstat.c pstat.h ../common/common.h
	$(MPICC) $(CFLAGS) $(MPICFLAGS) \
		-c pstat.c 

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x
	- /bin/rm -f core

cleanall:	clean
	- /bin/rm -f pstat

distclean:	cleanall
