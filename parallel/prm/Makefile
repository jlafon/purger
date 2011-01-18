# Customize these to your system
MPI_LD_FLAGS = -lmpi

CONFIG_PATH = ../../src
CC		=	gcc
CFLAGS		=	-Wall -D_GNU_SOURCE -g -pedantic -std=c99 -I$(CONFIG_PATH) -I../common/
CLIBS		=   -lconfig

MPICC		=	mpicc
MPICFLAGS	=	$(MPI_COMPILE_FLAGS) -g
MPICLIBS	=	$(MPI_LD_FLAGS)

PGLIBS		=	-L`pg_config --libdir` -lpq
PGINCLUDE	=	-I`pg_config --includedir`

UTILOBJS	=	../common/utils.o
OBJECTS		=	prm.o


all:	prm

prm:	$(OBJECTS) $(UTILOBJS)
	$(MPICC) $(MPICLIBS) $(PGLIBS) $(PGINCLUDE) \
		$(OBJECTS) $(UTILOBJS) $(CFLAGS) $(CLIBS) $(CONFIG_PATH)/config.o -o prm

prm.o:	../common/utils.o prm.c prm.h ../common/common.h
	$(MPICC) $(CFLAGS) $(MPICFLAGS)  \
		-c prm.c 

# Commented this out and included the common source directory above.  -Jharrod LaFon 1/3/11
#utils.o:	../common/utils.c ../common/utils.h ../common/common.h
#	$(CC) $(CFLAGS) -c utils.c

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x
	- /bin/rm -f core

cleanall:	clean
	- /bin/rm -f prm

distclean:	clean
	- /bin/rm -f prm
