from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from .Event import Event, EventFilter
from .PostUpsert import PostUpsert


@dataclass
class PostTimeFrameFilter(EventFilter):
    """Filter post upserts by created-at time range."""

    start: Optional[datetime] = None
    end: Optional[datetime] = None

    def apply(self, events: List[Event]) -> List[Event]:
        """
        Keep post upserts within `[start, end]` bounds.

        Args:
            events: Input events to evaluate.

        Returns:
            Filtered event list where non-post events always pass through.

        Raises:
            None.
        """
        if self.start is None and self.end is None:
            return events

        filtered: List[Event] = []
        for ev in events:
            if not isinstance(ev, PostUpsert):
                filtered.append(ev)
                continue

            if self.start is not None and ev.created_at < self.start:
                continue
            if self.end is not None and ev.created_at > self.end:
                continue

            filtered.append(ev)
        return filtered


@dataclass
class PostLanguageFilter(EventFilter):
    """Filter post upserts by language intersection."""

    langs: List[str]

    def __post_init__(self) -> None:
        """
        Normalize configured language set for case-insensitive matching.

        Args:
            None.

        Returns:
            None.

        Raises:
            None.
        """
        self._lang_set = {l.lower() for l in self.langs}

    def apply(self, events: List[Event]) -> List[Event]:
        """
        Keep post upserts that contain at least one configured language.

        Args:
            events: Input events to evaluate.

        Returns:
            Filtered event list where non-post events always pass through.

        Raises:
            None.
        """
        if not self._lang_set:
            return events

        filtered: List[Event] = []
        for ev in events:
            if not isinstance(ev, PostUpsert):
                filtered.append(ev)
                continue

            if not ev.langs:
                continue

            event_langs = {l.lower() for l in ev.langs if isinstance(l, str)}
            if event_langs.intersection(self._lang_set):
                filtered.append(ev)
        return filtered


@dataclass
class PostOriginalOnlyFilter(EventFilter):
    """Filter post upserts to original posts (exclude replies)."""

    def apply(self, events: List[Event]) -> List[Event]:
        """
        Keep only non-reply post upserts.

        Args:
            events: Input events to evaluate.

        Returns:
            Filtered event list where non-post events always pass through.

        Raises:
            None.
        """
        filtered: List[Event] = []
        for ev in events:
            if not isinstance(ev, PostUpsert):
                filtered.append(ev)
                continue

            if ev.reply_parent_uri is None:
                filtered.append(ev)
        return filtered
