# Changelog

## 0.1.0-alpha+003
### Changed
- Switched to Rust 2018 edition
- Refactoring code to avoid repeated use of `to_vec`
- In all of the protocol serializing implementations, `Vec::append`'s have been replaced with `[].concat()`'s. This removes the need to mutably create and access the `Vec` byte buffers when converting protocol structs to bytes.

## 0.1.0-alpha+002
### Added
- implementation of the API for CreateTopics
### Changed
- renaming BootstrapServer to KafkaServerAddr

## 0.1.0-alpha+001
- _not a real release, was originally in https://github.com/kdrakon/topiks_