# TODO: Do this in build.rs instead

possum.h:
	cbindgen --output $@ --lang c
