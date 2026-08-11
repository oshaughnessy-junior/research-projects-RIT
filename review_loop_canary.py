"""Disposable code-review loop canary."""

def parity_sign(mode):
    """Return +1 for even modes and -1 for odd modes."""
    return -1 if mode % 2 == 0 else 1
