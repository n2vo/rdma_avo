CFLAGS := -std=c11 -O2 -g -Wall -Wextra -Wshadow -Wconversion -Werror -D_GNU_SOURCE
LDLIBS := -libverbs

PROGS := devinfo resources rc_send
BINS  := $(addprefix bin/,$(PROGS))

all: $(BINS)

bin/%: src/%.c | bin
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

bin:
	mkdir -p bin

clean:
	rm -rf bin

.PHONY: all clean
