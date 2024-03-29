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

/**
 * Manages uncommitted writes
 */
typedef struct BatchWriter BatchWriter;

/**
 * Provides access to a storage directory. Manages manifest access, file cloning, file writers,
 * configuration, value eviction etc.
 */
typedef struct Handle Handle;

typedef struct PossumReader PossumReader;

/**
 * Represents a value obtained from a reader, before or after snapshot occurs.
 */
typedef struct PossumValue PossumValue;

typedef struct ValueWriter ValueWriter;

typedef BatchWriter PossumWriter;

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

Handle *possum_new(const char *path);

PossumError possum_start_new_value(PossumWriter *writer, PossumValueWriter **value);

RawFileHandle possum_value_writer_fd(PossumValueWriter *value);

PossumError possum_writer_rename(BatchWriter *writer, const PossumValue *value, PossumBuf new_key);

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

void possum_drop(Handle *handle);

PossumError possum_set_instance_limits(Handle *handle, const PossumLimits *limits);

PossumError possum_cleanup_snapshots(const Handle *handle);

size_t possum_single_write_buf(Handle *handle, PossumBuf key, PossumBuf value);

PossumWriter *possum_new_writer(Handle *handle);

bool possum_single_stat(const Handle *handle, PossumBuf key, PossumStat *out_stat);

PossumError possum_list_items(const Handle *handle,
                              PossumBuf prefix,
                              PossumItem **out_list,
                              size_t *out_list_len);

PossumError possum_single_read_at(const Handle *handle,
                                  PossumBuf key,
                                  PossumBuf *buf,
                                  uint64_t offset);

/**
 * stat is filled if non-null and a delete occurs. NoSuchKey is returned if the key does not exist.
 */
PossumError possum_single_delete(const Handle *handle, PossumBuf key, PossumStat *stat);

PossumError possum_reader_new(const Handle *handle, PossumReader **reader);

PossumError possum_handle_move_prefix(Handle *handle, PossumBuf from, PossumBuf to);

PossumError possum_handle_delete_prefix(Handle *handle, PossumBuf prefix);
