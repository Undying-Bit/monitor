"""
serial_parser.py - SAME/EAS header parsing from serial payloads.
"""
from __future__ import annotations

import re
from collections import defaultdict

from models import SerialParsed

HEADER_RE = re.compile(
    r"ZCZC-"
    r"(?P<org>[A-Z0-9]{3})-"
    r"(?P<eee>[A-Z0-9]{3})-"
    r"(?P<areas>[0-9]{6}(?:-[0-9]{6})*)"
    r"\+(?P<tttt>[0-9]{4})-"
    r"(?P<jjjhhmm>[0-9]{7})-"
    r"(?P<station>[A-Z0-9/]{3,8})-",
    re.IGNORECASE,
)


def parse_serial_payload(raw_payload: str) -> list[SerialParsed]:
    if not raw_payload:
        return []

    matches = list(HEADER_RE.finditer(raw_payload))
    if not matches:
        return []

    by_header: dict[str, list[re.Match]] = defaultdict(list)
    for m in matches:
        by_header[m.group(0)].append(m)

    parsed_list: list[SerialParsed] = []
    for header, group in by_header.items():
        m = group[0]
        org = m.group("org").upper()
        eee = m.group("eee").upper()
        areas_raw = m.group("areas")
        area_codes = areas_raw.split("-") if areas_raw else []
        tttt = m.group("tttt")
        jjjhhmm = m.group("jjjhhmm")
        julian_day = int(jjjhhmm[:3])
        hour = int(jjjhhmm[3:5])
        minute = int(jjjhhmm[5:7])
        station = m.group("station").upper()

        parsed_list.append(
            SerialParsed(
                originator=org,
                event_code=eee,
                area_codes=area_codes,
                duration_code=tttt,
                julian_day=julian_day,
                hour=hour,
                minute=minute,
                transmitter_code=station,
                raw_header=header,
                repeat_count=len(group),
            )
        )

    return parsed_list
