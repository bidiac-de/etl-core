from __future__ import annotations

from etl_core.components.databases.sql_reader_base import SQLReaderBase

from etl_core.components.databases.mariadb.mariadb import MariaDBComponent
from etl_core.components.component_registry import register_component
from etl_core.receivers.databases.mariadb.mariadb_receiver import MariaDBReceiver
from etl_core.components.wiring.ports import OutPortSpec


@register_component("read_mariadb")
class MariaDBRead(SQLReaderBase, MariaDBComponent):
    """MariaDB reader supporting row, bulk, and bigdata modes."""

    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    ALLOW_NO_INPUTS = True

    receiver_class = MariaDBReceiver
