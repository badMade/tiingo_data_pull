# GitHub Copilot Instructions

## Project Overview
This repository is designed for pulling financial market data from Tiingo API. It contains ticker symbol data and tools for fetching and processing financial information.

## Tech Stack
- **Language**: Python 3.10+
- **Testing**: pytest
- **Linting**: flake8
- **Code Quality**: SonarQube

## Coding Standards

### Style Guidelines
- Follow PEP 8 Python style guidelines
- Use flake8 for linting with these configurations:
  - Maximum line length: 127 characters
  - Maximum complexity: 10
  - Critical checks: E9, F63, F7, F82 (syntax errors, undefined names)
- Write clean, readable, and well-documented code
- Use meaningful variable and function names

### Code Organization
- Keep functions focused and single-purpose
- Avoid code duplication
- Use type hints where appropriate to improve code clarity

## Testing Requirements
- Write unit tests using pytest for all new functionality
- Tests should be placed in appropriate test directories
- Run tests before committing: `pytest`
- Ensure all tests pass before submitting pull requests

## Security Practices
- **Never** commit API keys, tokens, or credentials to the repository
- Use environment variables for sensitive configuration (e.g., `TIINGO_API_KEY`)
- Validate all external data inputs before processing
- Handle errors gracefully and log appropriately
- Keep dependencies up to date to avoid security vulnerabilities

## Dependencies
- Manage dependencies through `requirements.txt`
- Only add necessary dependencies
- Document why new dependencies are needed in pull requests
- Pin versions to ensure reproducible builds

## Documentation
- Add docstrings to all public functions and classes
- Use clear and descriptive commit messages
- Update relevant documentation when making changes
- Include inline comments for complex logic

## Data Handling
- When working with `all_tickers.json` or similar data files:
  - Validate JSON structure before processing
  - Handle missing or malformed data gracefully
  - Consider memory efficiency for large datasets
  - Use appropriate data structures for performance

## API Integration
- When working with Tiingo API:
  - Implement proper error handling and retries
  - Respect rate limits
  - Cache responses when appropriate
  - Log API calls for debugging

## Workflows and CI/CD
- The repository uses GitHub Actions for:
  - Python application testing (pytest and flake8)
  - SonarQube code quality analysis
- Ensure all CI checks pass before merging
- Fix any issues identified by automated checks

## Best Practices
- Write clean, maintainable code
- Prefer clarity over cleverness
- Use standard library functions when available
- Follow the principle of least surprise
- Keep it simple and straightforward
