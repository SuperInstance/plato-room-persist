"""JSONL room event journal."""

import json, time, os
from dataclasses import dataclass
from typing import Optional

@dataclass
class RoomEvent:
    event_type: str
    room: str
    agent: str = ""
    content: str = ""
    timestamp: float = 0.0

class RoomJournal:
    def __init__(self, path: str = None):
        self.path = path
        self._events: list[dict] = []
        if path and os.path.exists(path):
            self._load()

    def record(self, event_type: str, room: str, agent: str = "", content: str = "") -> dict:
        event = {"type": event_type, "room": room, "agent": agent,
                 "content": content, "timestamp": time.time()}
        self._events.append(event)
        if self.path:
            with open(self.path, "a") as f:
                f.write(json.dumps(event) + "\n")
        return event

    def enter(self, room: str, agent: str): return self.record("enter", room, agent)
    def leave(self, room: str, agent: str): return self.record("leave", room, agent)
    def message(self, room: str, agent: str, content: str): return self.record("message", room, agent, content)
    def state_change(self, room: str, content: str): return self.record("state_change", room, content=content)

    def events_since(self, timestamp: float) -> list[dict]:
        return [e for e in self._events if e.get("timestamp", 0) >= timestamp]

    def agents_in_room(self, room: str) -> list[str]:
        agents, in_room = [], set()
        for e in self._events:
            if e["room"] != room: continue
            if e["type"] == "enter" and e["agent"] not in in_room:
                in_room.add(e["agent"]); agents.append(e["agent"])
            elif e["type"] == "leave" and e["agent"] in in_room:
                in_room.discard(e["agent"])
        return [a for a in agents if a in in_room]

    def to_jsonl(self) -> str:
        return "\n".join(json.dumps(e) for e in self._events)

    def _load(self):
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if line:
                    self._events.append(json.loads(line))

    def compact(self, max_events: int = 10000):
        if len(self._events) > max_events:
            self._events = self._events[-max_events:]
            if self.path:
                with open(self.path, "w") as f:
                    for e in self._events:
                        f.write(json.dumps(e) + "\n")

    @property
    def stats(self) -> dict:
        rooms = set(e["room"] for e in self._events)
        return {"total": len(self._events), "rooms": len(rooms), "last_ts": self._events[-1]["timestamp"] if self._events else 0}
