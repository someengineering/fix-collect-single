from __future__ import annotations
from typing import Optional

from attr import define
from fixlib.types import Json


@define
class MetricQuery:
    name: str
    description: str
    search: str
    factor: Optional[int]
    only_on_collected_accounts: bool

    @staticmethod
    def from_json(name: str, js: Json) -> MetricQuery:
        return MetricQuery(
            name=name,
            description=js["description"],
            search=js["search"],
            factor=js.get("factor"),
            only_on_collected_accounts=js.get("only_on_collected_accounts", False),
        )
