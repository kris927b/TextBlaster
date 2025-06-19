// This file makes the `tests` directory a module.

// Declare sub-modules for test files.
// This makes their public items accessible from other integration tests
// via `crate::tests::producer_tests::...` etc.
pub mod producer_tests;

// If you had other test utility files, you'd add them here too:
// pub mod worker_tests;
// pub mod common_test_utils;
