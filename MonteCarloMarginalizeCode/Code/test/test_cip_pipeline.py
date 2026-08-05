from RIFT.misc.cip_pipeline import (
    POSTERIOR_REPLACEMENT_FLAG,
    use_replacement_before_final_iteration,
)


def test_replacement_is_enabled_only_before_final_actual_iteration():
    lines = [
        "2 --fit-method gp --parameter mc",
        "3 --fit-method rf --parameter mc",
    ]

    configured = use_replacement_before_final_iteration(lines)

    assert configured == [
        "2 --fit-method gp --parameter mc {}".format(POSTERIOR_REPLACEMENT_FLAG),
        "2 --fit-method rf --parameter mc {}".format(POSTERIOR_REPLACEMENT_FLAG),
        "1 --fit-method rf --parameter mc",
    ]


def test_single_final_iteration_keeps_unique_default():
    configured = use_replacement_before_final_iteration([
        "1 --fit-method gp",
        "1 --fit-method rf",
    ])

    assert POSTERIOR_REPLACEMENT_FLAG in configured[0]
    assert POSTERIOR_REPLACEMENT_FLAG not in configured[-1]


def test_gaussian_final_group_is_split_without_losing_prefix():
    configured = use_replacement_before_final_iteration([
        "G3 --fit-method quadratic",
    ])

    assert configured == [
        "G2 --fit-method quadratic {}".format(POSTERIOR_REPLACEMENT_FLAG),
        "G1 --fit-method quadratic",
    ]


def test_explicit_final_replacement_choice_is_preserved():
    configured = use_replacement_before_final_iteration([
        "2 --fit-method rf {}".format(POSTERIOR_REPLACEMENT_FLAG),
    ])

    assert POSTERIOR_REPLACEMENT_FLAG in configured[0]
    assert POSTERIOR_REPLACEMENT_FLAG in configured[-1]


def test_terminal_convergence_group_gets_unique_cleanup_iteration():
    configured = use_replacement_before_final_iteration([
        "2 --fit-method gp",
        "Z --fit-method rf",
    ])

    assert POSTERIOR_REPLACEMENT_FLAG in configured[0]
    assert configured[-2] == "Z --fit-method rf {}".format(
        POSTERIOR_REPLACEMENT_FLAG)
    assert configured[-1] == "1 --fit-method rf"
