CXX = g++
LDFLAGS = -pthread -lpmemobj -lpmem
DEFS = # -DMULT_SERVER # -DCAL_ACCESS_COUNT
WFLAGS = -Wall -Wno-format -Wno-unused-variable
CXXFLAGS += $(DEFS) $(IFLAGS) $(WFLAGS)
AR = ar
ARFLAGS = rs

LIB_SOURCES =  nvm_bptree.cc

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(LIB_SOURCES:.cc=)

LIBRARY = nvm_bptree.a

.PHONY: clean
default:  all

all:  clean test

clean: 
	rm -f $(LIBRARY)
	rm -f $(LIBOBJECTS)
	rm -f test

test:
	$(CXX) $(CXXFLAGS) test.cc $(LIB_SOURCES) -o test $(LDFLAGS);
