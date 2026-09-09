r"""Table and text rendering helpers"""

from __future__ import annotations

import re
import rich.highlighter
import rich.style
import rich.syntax
import rich.text

from collections import Counter


class StateHighlighter(rich.highlighter.Highlighter):
    STYLES = {
        "PENDING": "dim",
        "RUNNING": "cyan",
        "COMPLETED": "green",
        "FAILED": "red",
        "CANCELLED": "dark_orange",
        "UNKNOWN": "magenta",
    }

    def highlight(self, text: rich.text.Text) -> None:
        for match in re.finditer(r"\w+", text.plain):
            state, i, j = match.group(), match.start(), match.end()
            style = self.STYLES.get(state)

            if style:
                text.stylize(style, i, j)


def format_states(states: Counter[str] | str) -> rich.text.Text:
    r"""Formats the state(s) of one or several jobs as text."""

    if isinstance(states, str):
        text = states
    else:
        text = ", ".join(f"{count} {state}" for state, count in states.most_common())

    return StateHighlighter()(text)


class ANSITheme(rich.syntax.ANSISyntaxTheme):
    def __init__(self) -> None:
        super().__init__({
            token: style + rich.style.Style(bold=False)
            for token, style in rich.syntax.ANSI_DARK.items()
        })
