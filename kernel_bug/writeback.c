#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdio.h>

int main() {
	size_t const map_len = 0x1000;
	int w = open("writeback", O_RDWR|O_CREAT);
	if (w == -1) {
		perror("open");
		return 1;
	}
	if (-1 == ftruncate(w, map_len)) {
		perror("ftruncate");
		return 1;
	}
	off_t const seek_off = 2 * map_len;
	if (seek_off != lseek(w, seek_off, SEEK_SET)) {
		perror("lseek");
		return 1;
	}
	void *buf = mmap(NULL, map_len, PROT_READ, MAP_SHARED, w, 0);
	if (buf == MAP_FAILED) {
		perror("mmap");
		return 1;
	}
	write(w, buf, map_len);
	return 0;
}
