# program/library target and files
TARGET   = sybase_example
SRCS     = sybase_example.cpp

# explicit path to sybase library
LIBPATH  = -L/opt/sybase15/OCS-15_0/lib
INCLUDES = -I/opt/sybase15/OCS-15_0/include

# compiler path and flags
CC       = /opt/gcc/bin/g++
CFLAGS   = -std=c++1y -Wall -Wno-unused -Wno-sequence-point -Wno-parentheses -c -ggdb3 -m64 -pthread -DSYB_LP64 -D_REENTRANT

# sybase libraries to include in the build
CTLIB    = -lsybct64   # client library
CSLIB    = -lsybcs64   # cs library
TCLIB    = -lsybtcl64  # transport control layer
COMLIB   = -lsybcomn64 # internal shared utility library
INTLLIB  = -lsybintl64 # internationalization support library
UNICLIB  = -lsybunic64 # unicode library
BLKLIB   = -lsybblk64  # bulk copy routines
SYSLIBS  = -Wl,-Bdynamic -ldl -lpthread -lnsl -lm
LIBS     = -Bstatic $(LIBPATH) $(CTLIB) $(CSLIB) $(TCLIB) $(COMLIB) $(INTLLIB) $(UNICLIB) $(SYSLIBS)

# make env setup
OBJDIR   = obj
OBJECTS  = $(SRCS:.cpp=.o)
FPOBJS   = $(addprefix $(OBJDIR)/, $(SRCS:.cpp=.o))
vpath %.o $(OBJDIR)

# compilation rules
all: prep $(TARGET)

prep:
	rm -f make.log
	test -d $(OBJDIR) || mkdir $(OBJDIR)

.cpp.o:
	$(CC) $(CFLAGS) $(INCLUDES) $<  -o $(OBJDIR)/$(*).o 2>&1 | tee make.log

$(TARGET): $(OBJECTS)
	$(CC) -o $(TARGET) $(FPOBJS) $(LIBS) 2>&1 | tee make.log

clean:
	rm -f $(TARGET) $(OBJDIR)/*

depend:
	rm -f make.dep
	touch make.dep
	$(CC) $(CFLAGS) -MMD $(INCLUDES) $(SRCS) 2>&1 | tee -a make.log
