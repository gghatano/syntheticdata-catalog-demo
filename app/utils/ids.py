import datetime


def generate_id(prefix: str, sequence: int) -> str:
    """Generate a formatted ID like DS0001, SUB0001, EX0001."""
    return f"{prefix}{sequence:04d}"


def generate_timestamp() -> str:
    """Generate ISO format timestamp."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
