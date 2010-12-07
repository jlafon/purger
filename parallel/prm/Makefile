CC		=	gcc
CFLAGS		=	-Wall -D_GNU_SOURCE -g -pedantic -std=c99
CLIBS		=

MPICC		=	${MPI_ROOT}/bin/mpicc
MPICFLAGS	=	$(MPI_COMPILE_FLAGS) -g
MPICLIBS	=	$(MPI_LD_FLAGS)

PGLIBS		=	-L`pg_config --libdir` -lpq
PGINCLUDE	=	-I`pg_config --includedir`

UTILOBJS	=	utils.o
OBJECTS		=	prm.o


all:	prm

prm:	$(OBJECTS) $(UTILOBJS)
	$(MPICC) $(MPICLIBS) $(PGLIBS) $(PGINCLUDE) \
		$(OBJECTS) $(UTILOBJS) -o prm

prm.o:	utils.o prm.c prm.h common.h
	$(MPICC) $(CFLAGS) $(MPICFLAGS) \
		-c prm.c 

utils.o:	utils.c utils.h common.h
	$(CC) $(CFLAGS) -c utils.c

clean:
	- /bin/rm -f *~
	- /bin/rm -f *.o
	- /bin/rm -f *.x
	- /bin/rm -f core

cleanall:	clean
	- /bin/rm -f prm
