import os
from pathlib import Path


def load_env_file(env_path: str | os.PathLike | None = None, override: bool = True) -> None:
    """Load key/value pairs from .env into os.environ.

    By default .env wins over already exported machine variables. This keeps
    dev, Docker and app behavior aligned around the project-local .env file.
    """
    path = Path(env_path) if env_path else Path(__file__).resolve().parents[1] / ".env"
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        if line.startswith("export "):
            line = line[len("export "):].strip()

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = value
