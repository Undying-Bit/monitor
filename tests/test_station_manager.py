import pytest

from station_manager import StationManager


def test_station_manager_loads_valid_station_db(make_station_db):
    db_path = make_station_db(
        [
            (1, "ALFA", "+525511111111", "OPEN", "CLOSE", 0, "RED-A"),
            (2, "BETA", "+525522222222", "OPEN", "CLOSE", 1, "RED-B"),
        ]
    )

    manager = StationManager(db_path=db_path)

    assert manager.get_station("ALFA") is not None
    assert manager.get_station("ALFA").station_id == 1
    assert manager.lookup_by_phone("5511111111") == ["ALFA"]


def test_station_manager_requires_station_id_column(make_station_db):
    db_path = make_station_db(
        [("ALFA", "+525511111111", "OPEN", "CLOSE", 0, "RED-A")],
        include_station_id=False,
    )

    with pytest.raises(RuntimeError, match="station_id"):
        StationManager(db_path=db_path)


def test_station_manager_rejects_duplicate_station_id(make_station_db):
    db_path = make_station_db(
        [
            (1, "ALFA", "+525511111111", "OPEN", "CLOSE", 0, "RED-A"),
            (1, "BETA", "+525522222222", "OPEN", "CLOSE", 1, "RED-B"),
        ]
    )

    with pytest.raises(RuntimeError, match="Duplicate station_id"):
        StationManager(db_path=db_path)
