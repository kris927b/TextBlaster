# Contribution Guidelines

Thank you for your interest in contributing to this project! We welcome contributions from everyone.

To ensure a smooth contribution process, please follow these guidelines:

## How to Contribute

1.  **Fork the repository:** Start by forking the repository to your own GitHub account.
2.  **Clone the repository:** Clone your forked repository to your local machine.
    ```bash
    git clone https://github.com/your-username/rust-data.git
    ```
3.  **Create a new branch:** Create a new branch for your contribution. Use a descriptive name that indicates the purpose of your changes (e.g., `feat/add-contribution-guidelines`, `fix/broken-link`).
    ```bash
    git checkout -b your-branch-name
    ```
4.  **Make your changes:** Implement your changes, following the coding style and guidelines below.
5.  **Test your changes:** Ensure your changes pass all tests and do not introduce new issues.
6.  **Commit your changes:** Commit your changes with a clear and concise commit message.
    ```bash
    git commit -m "feat: Add contribution guidelines"
    ```
7.  **Push to your fork:** Push your changes to your forked repository.
    ```bash
    git push origin your-branch-name
    ```
8.  **Create a Pull Request:** Open a pull request from your branch to the `main` branch of the original repository. Provide a clear description of your changes and reference any related issues.

## Coding Style

We follow standard Rust coding conventions. Please ensure your code is formatted using `cargo fmt` and passes `cargo clippy`.

```bash
cargo fmt
cargo clippy
```

## Testing

All contributions should include appropriate tests. Ensure that existing tests pass and add new tests for any new functionality or bug fixes.

```bash
cargo test
```

## Pull Request Process

*   Ensure your pull request is based on the latest `main` branch.
*   Provide a clear title and description for your pull request.
*   Reference any related issues in your pull request description (e.g., `Closes #123`).
*   Be responsive to feedback on your pull request.

## Areas for Contribution

We welcome contributions in the following areas:

*   Adding new features
*   Improving existing functionality
*   Fixing bugs
*   Improving documentation
*   Adding more tests
*   Optimizing performance

If you have an idea for a contribution that doesn't fit into these categories, feel free to open an issue to discuss it first.

Thank you for helping to improve this project!
