"""Tests for dawgz.utils."""

import pytest

from dawgz.utils import cat


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
