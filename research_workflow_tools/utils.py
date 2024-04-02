from datetime import datetime


def generate_timestamp() -> str:
    """Generate a timestamp string

    Returns:
        str: Timestamp string
    """
    return datetime.now().strftime("%d-%m-%Y-%H:%M:%S")
