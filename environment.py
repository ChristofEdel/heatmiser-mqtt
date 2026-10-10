import os

#--------------------------------------------------------------------------------------------------
# region Environment Variable Helpers
#--------------------------------------------------------------------------------------------------


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value.strip()


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"Environment variable {name} must be an integer") from error


def env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as error:
        raise ValueError(f"Environment variable {name} must be a number") from error


def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def env_optional_int(name: str, default: int | None) -> int | None:
    value = os.environ.get(name)
    if value is None:
        return default
    if value.strip().lower() == "none":
        return None
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(
            f"Environment variable {name} must be an integer or 'none'"
        ) from error

# endregion
#--------------------------------------------------------------------------------------------------
