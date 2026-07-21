@echo off
rem Loads .env into this process's environment before running - gradlew/bootRun has no built-in
rem .env support (env_file: in docker-compose.yml is a Docker Compose convention, not a Gradle/
rem Spring Boot one), so without this, a local `run` picks up NONE of .env's values and silently
rem falls back to application.properties' hardcoded defaults - which have no password default at
rem all (intentionally, so a password is never hardcoded), so the DB connection fails auth.
if exist .env (
    for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do (
        if not "%%A"=="" if not "%%B"=="" set "%%A=%%B"
    )
)
call gradlew.bat clean bootRun
