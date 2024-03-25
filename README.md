# AWS Lambda Python DuckDB

## Requirements

- Python 3.12

## Development

- **Create a virtual environment**:

```sh
python3 -m venv venv
```

- **Run tests**:

```sh
cd app
pytest --cov --cov-report=html:coverage_report tests/
```

### Running github actions locally

- **Install**:

  - [GitHub CLI](https://cli.github.com/)
  - [GitHub CLI act plugin](https://nektosact.com/installation/gh.html)

- **run**:
  ```sh
  gh act
  ```
