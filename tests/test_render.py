"""Tests for dawgz.render."""

import pytest
import rich.text

from collections import Counter

from dawgz.render import format_states


@pytest.mark.parametrize(
    "states, expected",
    [
        ("COMPLETED", "COMPLETED"),
        ("UNKNOWN", "UNKNOWN"),
        (Counter({"COMPLETED": 2}), "2 COMPLETED"),
        (Counter({"COMPLETED": 2, "FAILED": 1}), "2 COMPLETED, 1 FAILED"),
        (Counter(), ""),
    ],
)
def test_format_states(states: Counter[str] | str, expected: str) -> None:
    assert format_states(states).plain == expected


def test_format_states_returns_text() -> None:
    assert isinstance(format_states("COMPLETED"), rich.text.Text)


def test_format_states_styles_known_states() -> None:
    text = format_states(Counter({"FAILED": 1}))

    assert "red" in str(text.spans[0].style)


def test_format_states_leaves_unknown_words_unstyled() -> None:
    text = format_states("NOTASTATE")

    assert text.spans == []
