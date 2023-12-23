# TODO: Do this in build.rs instead

.PHONY: possum.h
possum.h:
	cbindgen --output $@
