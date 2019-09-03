CXX = g++
LDFLAGS = -pthread -lpmemobj -lpmem
DEFS = -DSINGLE_BTREE
MULT_DEFS = 
WFLAGS = -Wall -Wno-format -Wno-unused-variable

SING_CXXFLAGS += $(DEFS) $(IFLAGS) $(WFLAGS)
MULT_CXXFLAGS += $(MULT_DEFS) $(IFLAGS) $(WFLAGS)

AR = ar
ARFLAGS = rs

SING_LIB_SOURCES = nvm_allocator.cc single_btree.cc nvm_btree.cc

MULT_LIB_SOURCES = nvm_allocator.cc con_btree.cc nvm_btree.cc

SKIPLIST_LIB_SOURCES = nvm_allocator.cc skiplist.cc

LIBOBJECTS = $(SING_LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(SING_LIB_SOURCES:.cc=)

LIBRARY = libnvmbtree.a

.PHONY: clean
default:  all

all:  clean single_test mult_test skiplist

clean: 
	rm -f $(LIBRARY)
	rm -f $(LIBOBJECTS)
	rm -f single_test mult_test

single_test:
	$(CXX) $(SING_CXXFLAGS) test.cc $(SING_LIB_SOURCES) -o single_test $(LDFLAGS)

mult_test:
	$(CXX) $(MULT_CXXFLAGS) test.cc $(MULT_LIB_SOURCES) -o mult_test $(LDFLAGS)

skiplist:
	$(CXX) $(MULT_CXXFLAGS) test_skiplist.cc $(SKIPLIST_LIB_SOURCES) -o skiplist_test $(LDFLAGS)

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -c $$sou_file.cc -o $$sou_file.o; \
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