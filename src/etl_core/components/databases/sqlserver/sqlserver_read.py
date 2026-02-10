from __future__ import annotations

from typing import ClassVar

from etl_core.components.databases.sql_reader_base import SQLReaderBase

from etl_core.components.databases.sqlserver.sqlserver import SQLServerComponent
from etl_core.components.component_registry import register_component
from etl_core.receivers.databases.sqlserver.sqlserver_receiver import SQLServerReceiver
from etl_core.components.wiring.ports import OutPortSpec


@register_component("read_sqlserver")
class SQLServerRead(SQLReaderBase, SQLServerComponent):
    """SQL Server reader supporting row, bulk, and bigdata modes."""

    OUTPUT_PORTS = (OutPortSpec(name="out", required=True, fanout="many"),)

    ALLOW_NO_INPUTS = True

    receiver_class: ClassVar[type] = SQLServerReceiver
