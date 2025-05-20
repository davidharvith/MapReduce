# Compiler and tools
CC       = g++
CXX      = g++
AR       = ar
ARFLAGS  = rcs
RANLIB   = ranlib

# Source and objects
LIBSRC   = MapReduceFramework.cpp GeneralContext.cpp Barrier.cpp  
LIBOBJ   = $(LIBSRC:.cpp=.o)

# Include paths and flags
INCS     = -I.
CFLAGS   = -Wall -std=c++11 -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -g $(INCS)

# Library name
OSMLIB   =  libMapReduceFramework.a

# Tarball settings
TAR = tar
TARNAME  = ex2.tar
TARFLAGS = -cvf
TARSRCS = $(LIBSRC) Barrier.h GeneralContext.h Makefile README

all: $(OSMLIB)

$(OSMLIB): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OSMLIB) $(LIBOBJ) *~ core

depend:
	makedepend -- $(CFLAGS) -- $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)