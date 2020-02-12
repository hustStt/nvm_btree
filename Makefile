CXX = g++
AR = ar
ARFLAGS = rs

LDFLAGS = -pthread -lpmemobj -lpmem
DEFS = -DSINGLE_BTREE -DNO_VALUE
MULT_DEFS = -DNO_VALUE
WFLAGS = -Wall -Wno-format -Wno-unused-variable

MULT_CXXFLAGS += $(MULT_DEFS) $(IFLAGS) $(WFLAGS)
MULT_LIB_SOURCES = nvm_allocator.cc con_btree.cc nvm_btree.cc

all: ycsb
	rm -rf test
	make -C src
	# ln -s src/single_test test
	# ln -s src/mult_test test
	ln -s src/nvtree_test test
clean:
	rm -rf test
	make clean -C src

install:
	make -C src $@
ycsb:
	$(CXX) $(MULT_CXXFLAGS) ycsb.cc $(MULT_LIB_SOURCES) -o mult_test $(LDFLAGS)

uninstall:
	make -C src $@