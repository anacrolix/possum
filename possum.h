#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>

typedef enum PossumError {
  NoError,
  NoSuchKey,
  SqliteError,
  IoError,
  AnyhowError,
} PossumError;

typedef struct BatchWriter BatchWriter;

typedef struct Handle Handle;

typedef struct PossumReader PossumReader;

typedef struct PossumValue PossumValue;

typedef struct ValueWriter ValueWriter;

typedef const char *KeyPtr;

typedef size_t KeySize;

typedef struct BatchWriter *PossumWriter;

typedef struct ValueWriter *PossumValueWriter;

typedef struct PossumTimestamp {
  int64_t secs;
  uint32_t nanos;
} PossumTimestamp;

typedef struct PossumStat {
  struct PossumTimestamp last_used;
  uint64_t size;
} PossumStat;

typedef struct possum_item {
  KeyPtr key;
  KeySize key_size;
  struct PossumStat stat;
} possum_item;

typedef struct PossumBuf {
  const char *ptr;
  size_t size;
} PossumBuf;

typedef uint64_t PossumOffset;

struct Handle *possum_new(const char *path);

void possum_drop(struct Handle *handle);

size_t possum_single_write_buf(struct Handle *handle,
                               KeyPtr key,
                               KeySize key_size,
                               const uint8_t *value,
                               size_t value_size);

PossumWriter possum_new_writer(struct Handle *handle);

enum PossumError possum_start_new_value(PossumWriter writer, PossumValueWriter *value);

int possum_value_writer_fd(PossumValueWriter value);

bool possum_single_stat(const struct Handle *handle,
                        KeyPtr key,
                        size_t key_size,
                        struct PossumStat *out_stat);

enum PossumError possum_list_keys(const struct Handle *handle,
                                  const unsigned char *prefix,
                                  size_t prefix_size,
                                  struct possum_item **out_list,
                                  size_t *out_list_len);

enum PossumError possum_single_readat(const struct Handle *handle,
                                      KeyPtr key,
                                      KeySize key_size,
                                      uint8_t *buf,
                                      size_t *nbyte,
                                      uint64_t offset);

enum PossumError possum_reader_new(const struct Handle *handle, struct PossumReader **reader);

enum PossumError possum_reader_add(struct PossumReader *reader,
                                   struct PossumBuf key,
                                   const struct PossumValue **value);

enum PossumError possum_reader_begin(struct PossumReader *reader);

/**
 * Consumes the
 */
enum PossumError possum_reader_end(struct PossumReader *reader);

enum PossumError possum_value_read_at(const struct PossumValue *value,
                                      struct PossumBuf *buf,
                                      PossumOffset offset);
