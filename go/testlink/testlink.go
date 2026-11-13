// Package testlink used to add the Rust target directory to the cgo library search path with a
// #cgo LDFLAGS -L directive. Go 1.27.4 and 1.26.11 scan directories named that way and reject the
// LLVM bitcode that Rust's prebuilt standard library leaves in libpossum.a, so the directive was
// removed. Pass the search path through the CGO_LDFLAGS environment variable instead, for example
// CGO_LDFLAGS=-L/path/to/possum/target/debug.
//
// Deprecated: importing this package no longer does anything.
package testlink
