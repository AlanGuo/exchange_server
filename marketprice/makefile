TARGET  := marketprice.exe
INCS = -I ../network -I ../utils
LIBS = -L ../utils -lutils -L ../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lssl -lcrypto -lhiredis -lcurl -lz -Wl,-Bdynamic -lm -lpthread -ldl -lrdkafka
include ../makefile.inc
