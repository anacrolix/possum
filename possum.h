#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct BatchWriter BatchWriter;

typedef struct Handle Handle;

struct Handle *possum_new(const char *path);

void possum_drop(struct Handle *handle);

size_t possum_single_write_buf(struct Handle *handle,
                               const unsigned char *key,
                               size_t key_size,
                               const uint8_t *value,
                               size_t value_size);

struct BatchWriter *possum_new_writer(struct Handle *handle);
