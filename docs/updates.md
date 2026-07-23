# Updates

## What This Page Is For

Use this page to update BEC without losing the persistent database volume.

## Update With Docker Compose

From the folder that contains `docker-compose.yml`:

Before deploying a Compose file that requires the SQLite web password, add a
strong, unique value to the existing `.env`:

```bash
openssl rand -base64 32
```

```ini
SQLITE_WEB_PASSWORD=""
```

Paste the generated value between the quotes. The deployment fails safely when
the variable is missing or empty.

```bash
docker compose pull
docker compose up -d
```

Then check the services:

```bash
docker compose ps
docker compose logs -f dashboard
docker compose logs -f jobs_runner
```

## Update With Dockge

If you manage the stack with Dockge:

1. open the BEC stack;
2. click **Update** or **Pull**;
3. redeploy the stack;
4. check logs and service health.

Keep Dockge private and admin-only.

## Do Not Delete The Volume

Avoid this command during normal updates:

```bash
docker compose down -v
```

It removes the persistent database volume, including BEC runtime data.

## Check The Changelog

Review recent changes before updating:

[BEC Changelog](https://github.com/jptsantossilva/BEC/blob/main/CHANGELOG.md)
