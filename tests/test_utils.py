"""Tests for dawgz.utils."""

import pytest

from datetime import datetime, timedelta

from dawgz.utils import at, cat, parse_duration, parse_timestamp


@pytest.mark.parametrize(
    "text, width, expected",
    [
        ("hello\nworld", -1, "hello\nworld"),
        ("abcd\refg", -1, "efgd"),
        ("abc\rxyz", -1, "xyz"),
        ("abcdef\r12", -1, "12cdef"),
        ("abc\r", -1, "abc"),
        ("\rabc", -1, "abc"),
        ("aaa\rbb\rc", -1, "cba"),
        ("line1\rX\nline2\ry", -1, "Xine1\nyine2"),
        ("a\n\nb", -1, "a\n\nb"),
        ("10%\r50%\r100%", -1, "100%"),
        ("abcdef", 2, "ab\ncd\nef"),
        ("aaaa\rbb", 2, "bb\naa"),
    ],
)
def test_cat(text: str, width: int, expected: str) -> None:
    assert cat(text, width) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("1w", timedelta(weeks=1)),
        ("3d", timedelta(days=3)),
        ("36h", timedelta(hours=36)),
        ("90m", timedelta(minutes=90)),
        ("30s", timedelta(seconds=30)),
        ("0h", timedelta()),
        ("2d12h", timedelta(days=2, hours=12)),
        ("1w10m", timedelta(weeks=1, minutes=10)),
        ("1w2d5h10m3s", timedelta(weeks=1, days=2, hours=5, minutes=10, seconds=3)),
    ],
)
def test_parse_duration(text: str, expected: timedelta) -> None:
    assert parse_duration(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "1",  # no unit
        "w",  # no amount
        "1h5",  # trailing amount
        "abc",
        "1y",  # years and months are not supported
        "1M",  # units are case-sensitive
        "1.5h",  # amounts are integers
        "-1h",  # amounts are non-negative
        "1h 1h",  # duplicate unit
        "1m1m",
        "2d1w",  # units must go from largest to smallest
        "1w 2d",  # pairs are not separated
        "1w_2d",
        "  1w",  # no surrounding whitespace
        "1w ",
        "1 w",  # no gap between an amount and its unit
        "1_w",
        "",  # no amount-unit pair at all
        "   ",
    ],
)
def test_parse_duration_invalid(text: str) -> None:
    with pytest.raises(ValueError, match="Invalid duration"):
        parse_duration(text)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("2025-01-02", datetime(2025, 1, 2)),
        ("2025-01-02 03:04:05", datetime(2025, 1, 2, 3, 4, 5)),
        ("1w", timedelta(weeks=1)),
        ("1w2d5h10m3s", timedelta(weeks=1, days=2, hours=5, minutes=10, seconds=3)),
    ],
)
def test_parse_timestamp(text: str, expected: datetime | timedelta) -> None:
    assert parse_timestamp(text) == expected


def test_parse_timestamp_invalid() -> None:
    with pytest.raises(ValueError, match="Invalid date-time or duration"):
        parse_timestamp("yesterday")


@pytest.mark.parametrize(
    "index, expected",
    [
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (-1, "c"),
        (-2, "b"),
        (-3, "a"),
    ],
)
def test_at(index: int, expected: str) -> None:
    assert at(["a", "b", "c"], index) == expected


@pytest.mark.parametrize("index", [3, 4, -4, 99, -99])
def test_at_out_of_range(index: int) -> None:
    with pytest.raises(IndexError, match="out of range"):
        at(["a", "b", "c"], index)


def test_at_reports_the_valid_range() -> None:
    with pytest.raises(IndexError, match=r"between -3 and 2"):
        at(["a", "b", "c"], 99)


def test_at_empty() -> None:
    with pytest.raises(IndexError, match="No element to index"):
        at([], 0)


def test_at_name_is_used_in_the_message() -> None:
    with pytest.raises(IndexError, match="Workflow index 9 is out of range"):
        at(["a", "b"], 9, "workflow")

    with pytest.raises(IndexError, match="No job to index"):
        at([], 0, "job")


def test_at_of_range_returns_the_resolved_index() -> None:
    # range(len(sequence)) is how callers recover the index itself.
    assert at(range(3), -1) == 2
    assert at(range(3), 0) == 0


def test_at_works_with_tuples_and_strings() -> None:
    assert at(("x", "y"), -1) == "y"
    assert at("abc", -2) == "b"
