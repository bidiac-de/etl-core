from __future__ import annotations

from etl_core.job_execution.job_execution_handler import JobExecutionHandler
import pytest

# Ensure stub components are registered with the component registry
# before Job tries to instantiate them.
import etl_core.components.stubcomponents as _ensure_registration  # noqa: F401
from tests.helpers import runtime_job_from_config


def test_extra_output_ports_are_visible_and_routable() -> None:
    """
    Dynamic output ports provided via config should:
    - appear in expected_ports()
    - be valid keys in routes
    - wire to the configured successors
    """
    job_cfg = {
        "name": "dyn-out-ports",
        "strategy_type": "row",
        "components": [
            {
                "name": "src",
                "comp_type": "test_source_dynamic_ports",
                "description": "",
                "routes": {"out": ["router"]},
            },
            {
                "name": "router",
                "comp_type": "test_router_dynamic_ports",
                "description": "",
                "extra_output_ports": ["ok", "err"],
                "routes": {
                    "ok": ["sink_ok"],
                    "err": ["sink_err"],
                },
            },
            {
                "name": "sink_ok",
                "comp_type": "test_sink_dynamic_ports",
                "description": "",
            },
            {
                "name": "sink_err",
                "comp_type": "test_sink_dynamic_ports",
                "description": "",
            },
        ],
    }

    job = runtime_job_from_config(job_cfg)

    router = next(c for c in job.components if c.name == "router")
    out_names = {p.name for p in router.expected_ports()}
    assert {"ok", "err"}.issubset(out_names)

    assert set(router.out_routes.keys()) >= {"ok", "err"}
    ok_targets = [c.name for c in router.out_routes["ok"]]
    err_targets = [c.name for c in router.out_routes["err"]]
    assert ok_targets == ["sink_ok"]
    assert err_targets == ["sink_err"]

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)
    assert execution.latest_attempt().error is None


def test_unknown_dynamic_out_port_in_routes_is_rejected() -> None:
    """
    If routes reference an out port that is NOT declared via class-level or
    extra_output_ports, wiring should fail with a clear error.
    """
    bad_cfg = {
        "name": "dyn-out-ports-bad",
        "strategy_type": "row",
        "components": [
            {
                "name": "src",
                "comp_type": "test_source_dynamic_ports",
                "description": "",
                "routes": {"out": ["router"]},
            },
            {
                "name": "router",
                "comp_type": "test_router_dynamic_ports",
                "description": "",
                "extra_output_ports": ["ok", "err"],
                "routes": {
                    "unknown": ["sink_ok"],
                },
            },
            {
                "name": "sink_ok",
                "comp_type": "test_sink_dynamic_ports",
                "description": "",
            },
        ],
    }

    with pytest.raises(ValueError) as ei:
        runtime_job_from_config(bad_cfg)
    msg = str(ei.value)
    assert (
        "unknown out port(s) in routes" in msg
        or "unknown out port(s) in port_schemas" in msg
    )


def test_dynamic_input_ports_require_in_port_when_ambiguous() -> None:
    """
    When a target declares multiple input ports (via extra_input_ports),
    an edge without an explicit in_port should be rejected. Supplying the
    in_port via EdgeRef should succeed.
    """
    # Ambiguous: missing in_port should fail
    ambiguous_cfg = {
        "name": "dyn-in-ports-ambiguous",
        "strategy_type": "row",
        "components": [
            {
                "name": "src",
                "comp_type": "test_source_dynamic_ports",
                "description": "",
                "routes": {"out": ["merge"]},
            },
            {
                "name": "merge",
                "comp_type": "test_merge_dynamic_inputs",
                "description": "",
                "extra_input_ports": ["left", "right"],
            },
        ],
    }
    with pytest.raises(ValueError) as ei1:
        runtime_job_from_config(ambiguous_cfg)
    assert "has multiple input ports" in str(ei1.value)

    # Explicit in_port: should pass and execute
    ok_cfg = {
        "name": "dyn-in-ports-ok",
        "strategy_type": "row",
        "components": [
            {
                "name": "src",
                "comp_type": "test_source_dynamic_ports",
                "description": "",
                "routes": {"out": [{"to": "merge", "in_port": "left"}]},
            },
            {
                "name": "merge",
                "comp_type": "test_merge_dynamic_inputs",
                "description": "",
                "extra_input_ports": ["left", "right"],
            },
        ],
    }
    job = runtime_job_from_config(ok_cfg)

    src = next(c for c in job.components if c.name == "src")
    in_ports_for_out = src.out_edges_in_ports.get("out", [])
    assert in_ports_for_out == ["left"]

    handler = JobExecutionHandler()
    execution = handler.execute_job(job)
    assert execution.latest_attempt().error is None
