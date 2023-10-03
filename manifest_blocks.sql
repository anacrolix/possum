-- This is a schema that would allow keys to be partially evicted by tracking usage at the block level.

create table keys (
    key_id integer,
    key blob,
);

create table values (
    key_id integer
    offset integer,
    value_part_id integer,
    primary key (key_id, "offset")
);

create table value_parts (
     value_part_id integer primary key,
     value_part_hash unique,
     block_id,
     block_offset,
     length,
);

create index block_value_parts (
    block_id,
    value_part_id,
);

create table blocks (
    block_id integer primary key,
    file_id integer,
    file_offset integer,
    block_length, // can vary by filesystem
    bytes_active,
    last_used integer,
);

create index block_eviction_index on blocks (
    bytes_active,
    last_used,
    block_id,
);