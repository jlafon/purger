CC		=	gcc
CFLAGS		=	-Wall -D_GNU_SOURCE -g
CLIBS		=

MPICC		=	${MPI_ROOT}/bin/mpicc
MPICFLAGS	=	$(MPI_COMPILE_FLAGS) -g
MPICLIBS	=	$(MPI_LD_FLAGS)

PGLIBS		=	-L`pg_config --libdir` -lpq
PGINCLUDE	=	-I`pg_config --includedir`

UTILOBJS	=	utils.o
OBJECTS		=	pstat.o


all:	utils.o

utils.o:	utils.c utils.h common.h
	$(CC) $(CFLAGS) -c utils.c

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x
	- /bin/rm -f core

cleanall:	clean

distclean:	clean
