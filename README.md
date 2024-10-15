# weave

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project), but instead of using pip, it uses [uv](https://github.com/astral-sh/uv).

## Getting started

There's no need to install stuff separately, create virtualenvs or manage python
versions - UV will install all the required dependencies when you try to run
something for the first time.

e.g. start the local Dagster UI web server in development mode:

```bash
uv run dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Secrets
```bash
cp .env.example .env
```

Then fill in the .env file with the necessary API keys

## Development

### Adding new Python dependencies

```bash
uv add dependency-name
```

### Unit testing

Tests are in the `weave_tests` directory and you can run tests using `pytest`:

```bash
uv run pytest weave_tests
```

### ADRs
Architectural Decision Records are in the `adrs` directory. They're Jupyter notebooks
which should run in the default `uv` venv.

## Deployment
The main branch is automatically deployed to dagster.io's serverless cloud hosting
through Github actions

Pull requests are deployed to a "branch deploy" where they can be tested independently