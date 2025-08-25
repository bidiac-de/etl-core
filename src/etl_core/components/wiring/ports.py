from __future__ import annotations

from typing import ClassVar, List, Literal, Sequence

from pydantic import BaseModel, Field

Fanout = Literal["one", "many"]
Fanin = Literal["one", "many"]


class OutPortSpec(BaseModel):
    """
    Declarative description of an output port.
    """

    name: str = Field(..., description="e.g. 'out', 'left', 'errors'")
    required: bool = Field(
        default=False, description="Must this port be routed to ≥1 successor?"
    )
    fanout: Fanout = Field(
        default="many", description="If 'one', enforce exactly 1 successor."
    )


class InPortSpec(BaseModel):
    """
    Declarative description of an input port.
    """

    name: str = Field(..., description="e.g. 'in', 'left', 'right'")
    required: bool = Field(
        default=False, description="Must at least one upstream edge land here?"
    )
    fanin: Fanin = Field(
        default="many", description="If 'one', enforce exactly 1 upstream edge."
    )


class DeclaresOutPorts:
    """
    Mixin: components declare output ports in PORTS.
    """

    OUTPUT_PORTS: ClassVar[Sequence[OutPortSpec]] = ()

    @classmethod
    def expected_ports(cls) -> List[OutPortSpec]:
        return list(cls.OUTPUT_PORTS)

    @classmethod
    def expected_port_names(cls) -> List[str]:
        return [p.name for p in cls.OUTPUT_PORTS]


class DeclaresInPorts:
    """
    Mixin: components declare input ports in INPUT_PORTS.
    """

    INPUT_PORTS: ClassVar[Sequence[InPortSpec]] = ()

    @classmethod
    def expected_in_ports(cls) -> List[InPortSpec]:
        return list(cls.INPUT_PORTS)

    @classmethod
    def expected_in_port_names(cls) -> List[str]:
        return [p.name for p in cls.INPUT_PORTS]


class EdgeRef(BaseModel):
    """
    Route target. 'to' is the component name. 'in_port' is the target input port.
    If omitted, wiring can default only if the target has exactly one input port.
    """

    to: str = Field(..., description="Target component name.")
    in_port: str | None = Field(
        default=None, description="Target input port name (if target has many)."
    )
