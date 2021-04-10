CXX = g++
LDFLAGS = -pthread -lpmemobj -lpmem -lrt -lm
DEFS = -DSINGLE_BTREE -DNO_VALUE
MULT_DEFS = -DNO_VALUE
WFLAGS = -Wall -Wno-format -Wno-unused-variable
CFLAGS = -ggdb

SING_CXXFLAGS += $(DEFS) $(IFLAGS) $(WFLAGS) $(CFLAGS)
MULT_CXXFLAGS += $(MULT_DEFS) $(IFLAGS) $(WFLAGS)

AR = ar
ARFLAGS = rs

SING_LIB_SOURCES = nvm_allocator.cc single_btree.cc nvm_btree.cc

PMDK_LIB_SOURCES = nvm_allocator.cc single_btree.cc nvm_btree.cc single_pmdk.cc

LIBOBJECTS = $(PMDK_LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(PMDK_LIB_SOURCES:.cc=)

LIBRARY = libnvmbtree.a

.PHONY: clean
default:  all

# all:  clean single_test mult_test skiplist nvtree
all: clean hbtree

cfile:
	rm -f /mnt/pmem1/mytest
	rm -f /mnt/pmem1/persistent
	rm -f /mnt/pmem1/log_persistent
	rm -f /mnt/pmem1/data

clean: 
	rm -f /mnt/pmem1/mytest
	rm -f /mnt/pmem1/persistent
	rm -f /mnt/pmem1/log_persistent
	rm -f $(LIBRARY)
	rm -f $(LIBOBJECTS)
	rm -f hbtree
	rm -f db_bench

hbtree:
	$(CXX) $(SING_CXXFLAGS) test.cc $(PMDK_LIB_SOURCES) -o hbtree $(LDFLAGS)

db_bench:
	$(CXX) $(SING_CXXFLAGS) db_bench.cc ../include/threadpool.cc ../include/histogram.cc $(PMDK_LIB_SOURCES) -o db_bench $(LDFLAGS)

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(SING_CXXFLAGS) $(LDFLAGS) -c $$sou_file.cc -o $$sou_file.o; \
	done;

$(LIBRARY) : $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^

INSTALL_PATH := /usr/local
INCLUDE_PATH := nvmbtree

install-headers:
	install -d $(INSTALL_PATH)/lib
	install -d $(INSTALL_PATH)/include/$(INCLUDE_PATH)
	for header in `find "." -type f -name "*.h"`; do \
		install -C -m 644 $$header $(INSTALL_PATH)/include/$(INCLUDE_PATH)/$$header; \
	done

install-static: install-headers $(LIBRARY)
	install -C -m 755 $(LIBRARY) $(INSTALL_PATH)/lib

install:install-static

uninstall:
	rm -rf $(INSTALL_PATH)/include/$(INCLUDE_PATH) \
	$(INSTALL_PATH)/lib/$(LIBRARY) \