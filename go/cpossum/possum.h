#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>

typedef enum {
  NoError,
  NoSuchKey,
  SqliteError,
  IoError,
  AnyhowError,
  UnsupportedFilesystem,
} PossumError;

typedef struct Arc_RwLock_Handle Arc_RwLock_Handle;

/**
 * Manages uncommitted writes
 */
typedef struct BatchWriter_PossumHandle BatchWriter_PossumHandle;

typedef struct PossumReader PossumReader;

/**
 * Represents a value obtained from a reader, before or after snapshot occurs.
 */
typedef struct PossumValue PossumValue;

typedef struct ValueWriter ValueWriter;

typedef Arc_RwLock_Handle PossumHandleRc;

typedef PossumHandleRc PossumHandle;

typedef BatchWriter_PossumHandle PossumWriter;

typedef ValueWriter PossumValueWriter;

typedef intptr_t RawFileHandle;

typedef struct {
  const char *ptr;
  size_t size;
} PossumBuf;

typedef uint64_t PossumOffset;

typedef struct {
  int64_t secs;
  uint32_t nanos;
} PossumTimestamp;

typedef struct {
  PossumTimestamp last_used;
  uint64_t size;
} PossumStat;

typedef struct {
  PossumBuf key;
  PossumStat stat;
} PossumItem;

typedef struct {
  uint64_t max_value_length_sum;
  bool disable_hole_punching;
} PossumLimits;

PossumHandle *possum_new(const char *path);

PossumError possum_start_new_value(PossumWriter *writer, PossumValueWriter **value);

RawFileHandle possum_value_writer_fd(PossumValueWriter *value);

PossumError possum_writer_rename(PossumWriter *writer, const PossumValue *value, PossumBuf new_key);

PossumError possum_reader_add(PossumReader *reader, PossumBuf key, const PossumValue **value);

/**
 * Takes a snapshot so the reader values can be used.
 */
PossumError possum_reader_begin(PossumReader *reader);

/**
 * Consumes the reader, invalidating all values produced from it.
 */
void possum_reader_end(PossumReader *reader);

PossumError possum_value_read_at(const PossumValue *value, PossumBuf *buf, PossumOffset offset);

void possum_value_stat(const PossumValue *value, PossumStat *out_stat);

PossumError possum_reader_list_items(const PossumReader *reader,
                                     PossumBuf prefix,
                                     PossumItem **out_items,
                                     size_t *out_len);

PossumError possum_writer_commit(PossumWriter *writer);

PossumError possum_writer_stage(PossumWriter *writer, PossumBuf key, PossumValueWriter *value);

void possum_drop(PossumHandle *handle);

PossumError possum_set_instance_limits(PossumHandle *handle, const PossumLimits *limits);

PossumError possum_cleanup_snapshots(const PossumHandle *handle);

size_t possum_single_write_buf(PossumHandle *handle, PossumBuf key, PossumBuf value);

PossumWriter *possum_new_writer(PossumHandle *handle);

bool possum_single_stat(const PossumHandle *handle, PossumBuf key, PossumStat *out_stat);

PossumError possum_list_items(const PossumHandle *handle,
                              PossumBuf prefix,
                              PossumItem **out_list,
                              size_t *out_list_len);

PossumError possum_single_read_at(const PossumHandle *handle,
                                  PossumBuf key,
                                  PossumBuf *buf,
                                  uint64_t offset);

/**
 * stat is filled if non-null and a delete occurs. NoSuchKey is returned if the key does not exist.
 */
PossumError possum_single_delete(const PossumHandle *handle, PossumBuf key, PossumStat *stat);

PossumError possum_reader_new(const PossumHandle *handle, PossumReader **reader);

PossumError possum_handle_move_prefix(PossumHandle *handle, PossumBuf from, PossumBuf to);

PossumError possum_handle_delete_prefix(PossumHandle *handle, PossumBuf prefix);
