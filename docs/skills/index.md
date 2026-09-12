# Using Skills with Bob / Claude

The `skills/` directory contains `SKILL.md` files that teach Bob/Claude agents how to invoke
the CLI tools in this repository via shell commands.

## Available Skills

| Skill | File | Covers |
|-------|------|--------|
| `flink-deploy` | `skills/flink-deploy/SKILL.md` | Deploy/undeploy Flink statements, snapshot/streaming queries |
| `manifest` | `skills/manifest/SKILL.md` | Generate `deploy_manifest.json` |
| `dbt-project` | `skills/dbt-project/SKILL.md` | Migrate Flink DML → dbt, scaffold dbt projects |
| `kafka` | `skills/kafka/SKILL.md` | Schema Registry, table cleanup |

## Installing Skills in Bob

Copy or symlink the skills directory into your Bob workspace:

```bash
./scripts/link-skills.sh ~/.bob/skills
# OR
./scripts/link-skills.sh ~/.claude/skills
# other harnesses compatible with skill spec
./scripts/link-skills.sh ~/.agents/skills
```

Reload Bob, or start claude, pi...

## How Skills Work

Each skill instructs Bob to invoke the tools via `uv run` shell commands. The skill handles:

1. **Credential setup** — checking required env vars are set before invoking any tool
2. **Workflow guidance** — step-by-step instructions (e.g. generate manifest → deploy)
3. **Error interpretation** — common error patterns and their resolutions
4. **Dry-run first** — always suggests `--dry-run` before write operations

## Trigger Phrases

Each skill activates on specific phrases. Examples:

| Phrase | Skill triggered |
|--------|----------------|
| "deploy my Flink SQL" | `flink-deploy` |
| "generate a deploy manifest" | `manifest` |
| "migrate my DML to dbt" | `dbt-project` |
| "scaffold a dbt project" | `dbt-project` |
| "register my Avro schema" | `kafka` |
| "clean up Flink tables" | `kafka` |
