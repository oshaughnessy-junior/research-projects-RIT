"""Helpers for constructing iteration-specific CIP argument groups."""


POSTERIOR_REPLACEMENT_FLAG = "--posterior-resample-with-replacement"


def _add_replacement_flag(line):
    """Add the replacement flag once while preserving the iteration prefix."""
    line = line.rstrip()
    if POSTERIOR_REPLACEMENT_FLAG in line.split():
        return line
    return "{} {}".format(line, POSTERIOR_REPLACEMENT_FLAG)


def use_replacement_before_final_iteration(lines):
    """Enable fair draws internally and preserve a unique final CIP grid.

    CIP argument files group repeated iterations by prefixing each argument line
    with a count (or ``G<count>`` for Gaussian-resampling iterations).  Split a
    repeated final group so its internal repetitions use replacement while the
    last actual iteration retains CIP's default, sampling without replacement.

    A terminal ``Z`` group runs an indeterminate number of convergence
    iterations.  Keep replacement enabled throughout that sub-DAG and append
    one ordinary cleanup iteration to produce the unique final intrinsic grid.
    """
    lines = [line.rstrip() for line in lines if line.strip()]
    if not lines:
        return []

    configured = [_add_replacement_flag(line) for line in lines[:-1]]
    final_line = lines[-1]
    parts = final_line.split(maxsplit=1)
    prefix = parts[0]
    arguments = parts[1] if len(parts) == 2 else ""

    if prefix == "Z":
        configured.append(_add_replacement_flag(final_line))
        configured.append("1 {}".format(arguments).rstrip())
        return configured

    gaussian = prefix.startswith("G")
    count_text = prefix[1:] if gaussian else prefix
    try:
        count = int(count_text)
    except ValueError:
        # Preserve unknown helper extensions rather than changing their meaning.
        configured.append(final_line)
        return configured

    if count > 1:
        internal_prefix = "{}{}".format("G" if gaussian else "", count - 1)
        configured.append(_add_replacement_flag(
            "{} {}".format(internal_prefix, arguments).rstrip()))

    final_prefix = "{}1".format("G" if gaussian else "")
    configured.append("{} {}".format(final_prefix, arguments).rstrip())
    return configured
