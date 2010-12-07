all:
	- cd src && make
	- cd serial && make

serial:
	- cd src && make
	- cd serial && make

parallel:
	- cd src && make
	- cd parallel && make

clean:
	- cd src && make clean
	- cd serial && make clean
	- rm -f *~

distclean:
	- cd src && make distclean
	- cd serial && make distclean
	- rm -f *~
