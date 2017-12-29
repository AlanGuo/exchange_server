TARGET  := monitorcenter.exe
INCS = -I ../network -I ../utils
LIBS = -L ../utils -lutils -L ../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lz -lssl -lcrypto -lhiredis -lcurl -Wl,-Bdynamic -lm -lpthread -ldl -lrdkafka
include ../makefile.inc
