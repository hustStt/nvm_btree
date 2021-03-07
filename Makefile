CXX = g++
AR = ar
ARFLAGS = rs

LDFLAGS = -pthread -lpmemobj -lpmem -ltbb
DEFS = -DSINGLE_BTREE -DNO_VALUE
MULT_DEFS = -DNO_VALUE
WFLAGS = -Wall -Wno-format -Wno-unused-variable
CFLAGS = -msse4.2 -mpclmul -march=native -funroll-loops -Wstrict-overflow -Wstrict-aliasing -Wall -Wextra -pedantic -Wshadow

MULT_CXXFLAGS += $(MULT_DEFS) $(IFLAGS) $(WFLAGS) $(CFLAGS)
MULT_LIB_SOURCES = src/nvm_allocator.cc src/single_btree.cc src/single_pmdk.cc fptree/clhash.c fptree/fptree.cc fptree/utility.cpp fptree/p_allocator.cpp

all: ycsb
	rm -rf test
	# make -C src
	# ln -s src/single_test test
	# ln -s src/mult_test test
	# ln -s src/pmdk_test test
clean:
	rm -rf /mnt/pmem0/ycsb
	rm -rf /mnt/pmem0/persistent
	rm -rf /mnt/pmem0/log_persistent
	rm -rf ycsb
	make clean -C src

install:
	make -C src $@
ycsb:
	$(CXX) $(MULT_CXXFLAGS) ycsb.cc $(MULT_LIB_SOURCES) -o ycsb $(LDFLAGS)

uninstall:
	make -C src $@