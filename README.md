# [flink-tools-for-agents](https://jbcodeforce.github.io/flink-tools-for-agents)

Reusable CLI tools and Bob/Claude agent skills for managing Confluent Cloud Flink projects, SQL statements, dbt confluent projects, and Kafka on Confluent Cloud.


## Installation

* Requires Python ≥ 3.12 and [uv](https://docs.astral.sh/uv/).
* Work from dbt folder

```bash
# Install all domains
uv sync --extra all

# Install a single domain
uv sync --extra flink
uv sync --extra dbt
uv sync --extra kafka
```

## User Guides

[Read the book format](https://jbcodeforce.github.io/flink-tools-for-agents)

## Configuration

All tools load credentials from environment variables or a `.env` file.
Set `CONFLUENT_ENV_FILE=/path/to/.env` to point to a custom location, or place `.env` in the repo root.

Required variables per domain are documented in each skill's `SKILL.md` file.

## Development

```bash
make install   # uv sync --extra all --extra dev
make test      # uv run pytest
make lint      # uv run ruff check .
make docs      # uv run mkdocs serve
```

## Skills for Bob / Claude

Install the skills by copying or symlinking the `skills/` directory into your `.bob/skills/` folder,
or reference individual `SKILL.md` files from your Bob workspace configuration.

## License

Apache 2.0
