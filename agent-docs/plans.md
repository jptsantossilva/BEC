# Planning Guide for Agents

Use this guide for tasks that are large, risky, cross-module, or likely to take
multiple implementation steps.

## When to Write a Plan
- Major refactors across `bec/`, `pages/`, Docker, or docs.
- Changes to trading execution, persistence, scheduling, or backtesting rules.
- Tasks where tests, migrations, rollout, or compatibility need coordination.
- Any task where the user asks for a plan before implementation.

## Plan Shape
Keep plans decision-complete and concise:

1. State the goal and success criteria.
2. List the subsystems that will change.
3. Describe the implementation approach at behavior level.
4. Call out compatibility, data, or safety constraints.
5. Define validation commands and manual checks.
6. Record assumptions that should be revisited if they prove false.

## Execution Notes
- Read the relevant code before planning implementation details.
- Prefer existing project patterns over new abstractions.
- Keep root compatibility wrappers stable unless the task explicitly targets
  Docker, Streamlit, cron, or scheduler entrypoints.
- Update the plan if implementation reveals a different architecture or risk.
- End with the exact tests/checks that were run or could not be run.

## Validation Checklist
- `python -m pytest tests` for Python behavior changes.
- `python -m py_compile bec/*.py bec/utils/*.py bec/exchanges/*.py bec/signals/*.py`
  for broad syntax validation.
- `npm run docs:build` for documentation changes.
- Manual dashboard check for Streamlit UI changes.
- Docker service/log check for container, entrypoint, or compose changes.
