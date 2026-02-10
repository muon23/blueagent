from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from .Event import Event, EventFilter
from .PostUpsert import PostUpsert


@dataclass
class PostTimeFrameFilter(EventFilter):
    start: Optional[datetime] = None
    end: Optional[datetime] = None

    def apply(self, events: List[Event]) -> List[Event]:
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
    langs: List[str]

    def __post_init__(self) -> None:
        self._lang_set = {l.lower() for l in self.langs}

    def apply(self, events: List[Event]) -> List[Event]:
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
    def apply(self, events: List[Event]) -> List[Event]:
        filtered: List[Event] = []
        for ev in events:
            if not isinstance(ev, PostUpsert):
                filtered.append(ev)
                continue

            if ev.reply_parent_uri is None:
                filtered.append(ev)
        return filtered
