from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest


@pytest.fixture
def workspace_tmp_dir() -> Path:
    root = Path(__file__).resolve().parents[1] / "tmp" / f"pytest-{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def parsed_db_path(workspace_tmp_dir: Path) -> Path:
    return workspace_tmp_dir / "mensajes.db"


@pytest.fixture
def make_station_db(workspace_tmp_dir: Path):
    def _make(
        rows: list[tuple],
        *,
        include_station_id: bool = True,
    ) -> Path:
        db_path = workspace_tmp_dir / "estaciones.db"

        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            if include_station_id:
                cursor.execute(
                    """
                    CREATE TABLE Estaciones (
                        station_id INTEGER,
                        nombre TEXT,
                        telefono TEXT,
                        "open" TEXT,
                        "close" TEXT,
                        "tx sarmex" INTEGER,
                        red TEXT
                    )
                    """
                )
                cursor.executemany(
                    """
                    INSERT INTO Estaciones
                        (station_id, nombre, telefono, "open", "close", "tx sarmex", red)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
            else:
                cursor.execute(
                    """
                    CREATE TABLE Estaciones (
                        nombre TEXT,
                        telefono TEXT,
                        "open" TEXT,
                        "close" TEXT,
                        "tx sarmex" INTEGER,
                        red TEXT
                    )
                    """
                )
                cursor.executemany(
                    """
                    INSERT INTO Estaciones
                        (nombre, telefono, "open", "close", "tx sarmex", red)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
            conn.commit()

        return db_path

    return _make
