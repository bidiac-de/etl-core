from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest
import sys
import types
import etl_core.receivers.files.xml.xml_helper as xh


@pytest.fixture(autouse=True)
def patch_file_helpers(monkeypatch):
    monkeypatch.setattr(xh, "resolve_file_path", lambda p: Path(p))

    def _open_file(path: Path, mode: str = "r", *args, **kwargs):
        return open(path, mode, *args, **kwargs)

    monkeypatch.setattr(xh, "open_file", _open_file)


def _mk_xml(root: str, items: List[str]) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n<'
        + root
        + ">"
        + "".join(items)
        + f"</{root}>"
    )


def _el(tag: str, inner: str) -> str:
    return f"<{tag}>{inner}</{tag}>"


def _canonical_xml(txt: str) -> str:
    """
    Normalize common serializer variations so we can compare robustly:
    - collapse ' />' to '/>'
    - unify the XML decl to single-quoted variant
    """
    txt = txt.replace(" />", "/>")
    import re

    txt = re.sub(
        r'<\?xml version=["\']1\.0["\'] encoding=["\']utf-8["\']\?>',
        "<?xml version='1.0' encoding='utf-8'?>",
        txt,
        flags=re.IGNORECASE,
    )
    return txt


def test_element_to_nested_with_attrs_text_repeat():
    xml = (
        '<book id="42" lang="en">'
        "  intro"
        "  <author>A</author>"
        "  <author><first>F</first><last>L</last></author>"
        '  <meta key="x"/>'
        "</book>"
    )
    el = xh.ET.fromstring(xml)
    got = xh.element_to_nested(el)
    assert got[xh.ATTRS] == {"id": "42", "lang": "en"}
    assert got[xh.TEXT] == "intro"
    assert got["author"][0] == "A"
    assert got["author"][1] == {"first": "F", "last": "L"}
    assert got["meta"][xh.ATTRS]["key"] == "x"


def test_element_to_nested_leaf_only():
    el = xh.ET.fromstring("<x>  hi  </x>")
    assert xh.element_to_nested(el) == "hi"


@pytest.mark.parametrize(
    ("payload", "expected_xml"),
    [
        ({"a": "1"}, "<root><a>1</a></root>"),
        ({xh.ATTRS: {"id": 7}, xh.TEXT: "hi"}, '<root id="7">hi</root>'),
        (
            {"item": ["x", {xh.ATTRS: {"k": "v"}}]},
            '<root><item>x</item><item k="v" /></root>',
        ),
        (None, "<root />"),
    ],
)
def test_nested_to_element_builds_expected(
    payload: Dict[str, Any] | None, expected_xml: str
):
    el = xh.nested_to_element("root", payload)
    got = xh.ET.tostring(el, encoding="unicode")
    got_norm = _canonical_xml(got)
    exp_norm = _canonical_xml(expected_xml)
    assert got_norm == exp_norm


def test_flatten_and_unflatten_roundtrip_with_attrs_and_lists():
    nested = {
        xh.ATTRS: {"id": "1"},
        xh.TEXT: "t",
        "a": {"b": 2, "c": [3, {"d": 4}]},
    }
    flat = xh._flatten_record(nested)
    assert flat["@attrs.id"] == "1"
    assert flat["#text"] == "t"
    assert flat["a.b"] == 2
    assert flat["a.c[0]"] == 3
    assert flat["a.c[1].d"] == 4

    un = xh.unflatten_record(flat)
    assert un == nested


def test_unflatten_record_steps_all_paths():
    flat = {
        "a": 1,
        "b[0]": {"x": 2},
        "b[1].y": 3,
        f"{xh.ATTRS}.k": "v",
        xh.TEXT: "hello",
    }
    un = xh.unflatten_record(flat)
    assert un["a"] == 1
    assert un["b"][0] == {"x": 2}
    assert un["b"][1]["y"] == 3
    assert un[xh.ATTRS]["k"] == "v"
    assert un[xh.TEXT] == "hello"


def test_build_payload_type_errors_on_non_dict():
    with pytest.raises(TypeError):
        xh.build_payload(["not", "a", "dict"])  # type: ignore[arg-type]


def test_build_payload_flat_filters_nullish_list_items():
    flat = {"tags[0]": "a", "tags[1]": None, "name": None}
    out = xh.build_payload(flat)
    assert out.get("tags") == ["a"]
    assert "name" in out and out["name"] is None


def test_build_payload_uses_isna_but_falls_back_on_exception(monkeypatch):
    def _boom(_):
        raise RuntimeError("boom")

    monkeypatch.setattr(xh.pd, "isna", _boom)
    flat = {"tags[0]": None, "keep": None}
    out = xh.build_payload(flat)
    assert out.get("keep") is None
    assert ("tags" not in out) or (out["tags"] == [])


def test_build_payload_passthrough_nested():
    nested = {"a": {"b": 1}}
    assert xh.build_payload(nested) == nested


def test_has_flat_paths_detects_dots_and_brackets():
    assert xh._has_flat_paths({"a.b": 1})
    assert xh._has_flat_paths({"a[0]": 1})
    assert not xh._has_flat_paths({"a": 1})


def test_read_xml_row_and_flatten_chunks_and_once(tmp_path: Path):
    recs = [
        _el("row", _el("id", "1") + _el("name", "A")),
        _el("row", _el("id", "2") + _el("name", "B")),
        _el("row", _el("id", "3") + _el("name", "C")),
    ]
    p = tmp_path / "in.xml"
    p.write_text(_mk_xml("rows", recs), encoding="utf-8")

    rows = list(xh.read_xml_row(p, "row"))
    assert rows == [
        {"id": "1", "name": "A"},
        {"id": "2", "name": "B"},
        {"id": "3", "name": "C"},
    ]

    parts = list(xh.read_xml_bulk_chunks(p, "row", chunk_size=2))
    assert [len(df) for df in parts] == [2, 1]
    once = xh.read_xml_bulk_once(p, "row")
    assert once.shape == (3, 2)
    assert set(once.columns) == {"id", "name"}


def test_read_xml_bulk_once_empty_returns_empty_df(tmp_path: Path):
    p = tmp_path / "empty.xml"
    p.write_text(_mk_xml("rows", []), encoding="utf-8")
    df = xh.read_xml_bulk_once(p, "row")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_write_xml_bulk_writes_root_and_records(tmp_path: Path):
    p = tmp_path / "out.xml"
    df = pd.DataFrame(
        [
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
        ]
    )
    xh.write_xml_bulk(p, df, root_tag="rows", record_tag="row")
    txt = p.read_text(encoding="utf-8")
    assert txt.startswith("<?xml ")
    assert "<rows>" in txt
    assert "<row><id>1</id><name>A</name></row>" in txt
    assert "<row><id>2</id><name>B</name></row>" in txt
    assert txt.strip().endswith("</rows>")


def test_write_xml_bulk_with_empty_df_writes_empty_root(tmp_path: Path):
    p = tmp_path / "empty.xml"
    xh.write_xml_bulk(p, pd.DataFrame([]), root_tag="rows", record_tag="row")
    txt = p.read_text(encoding="utf-8").strip()
    assert txt.startswith("<?xml ")
    assert txt.endswith("<rows />") or txt.endswith("<rows/>")


def test_write_xml_row_validations(tmp_path: Path):
    p = tmp_path / "rows.xml"

    with pytest.raises(TypeError):
        xh.write_xml_row(p, ["not", "dict"], root_tag="rows", record_tag="row")

    with pytest.raises(ValueError):
        xh.write_xml_row(p, {"a.b": 1}, root_tag="rows", record_tag="row")

    xh.write_xml_row(p, {"id": "1", "name": "A"}, root_tag="rows", record_tag="row")
    txt = p.read_text(encoding="utf-8")
    assert "<rows>" in txt and "</rows>" in txt
    assert "<row><id>1</id><name>A</name></row>" in txt


def test_append_record_to_new_file_creates_root(tmp_path: Path):
    p = tmp_path / "new.xml"
    el = xh.nested_to_element("row", {"id": "1"})
    xh._append_record_to_file(p, "rows", el)
    t = p.read_text(encoding="utf-8")
    assert "<rows><row><id>1</id></row></rows>" in _canonical_xml(t)


def test_append_record_scans_from_end(tmp_path: Path):
    p = tmp_path / "rows.xml"
    base = _mk_xml("rows", [_el("row", _el("id", "1")), _el("row", _el("id", "2"))])
    p.write_text(base, encoding="utf-8")

    el = xh.nested_to_element("row", {"id": "3"})
    xh._append_record_to_file(p, "rows", el)
    t = p.read_text(encoding="utf-8")
    assert "<row><id>1</id></row>" in t
    assert "<row><id>2</id></row>" in t
    assert "<row><id>3</id></row>" in t
    assert t.strip().endswith("</rows>")


@pytest.mark.skipif(
    os.name == "nt", reason="Windows prevents os.replace on an open file handle."
)
def test_append_record_fallback_when_closing_not_found(tmp_path: Path, monkeypatch):
    p = tmp_path / "broken.xml"
    p.write_text(_mk_xml("rows", [_el("row", _el("id", "1"))]), encoding="utf-8")

    monkeypatch.setattr(xh, "_closing_tag_bytes_for", lambda *_: b"</notfound>")

    el = xh.nested_to_element("row", {"id": "9"})
    xh._append_record_to_file(p, "rows", el)


class _DummyFile:
    def __init__(self) -> None:
        self._pos = 0

    def fileno(self) -> int:
        return 0

    def seek(self, pos: int, whence: int = os.SEEK_SET) -> None:
        if whence == os.SEEK_SET:
            self._pos = pos
        elif whence == os.SEEK_CUR:
            self._pos += pos
        else:
            self._pos = pos


def test_detect_root_bytes_decl_and_none(tmp_path: Path) -> None:
    has_decl = tmp_path / "with_decl.xml"
    has_decl.write_text(
        '<?xml version="1.0" encoding="utf-8"?>\n<rows><row /></rows>',
        encoding="utf-8",
    )
    no_xml = tmp_path / "garbage.txt"
    no_xml.write_text("not xml at all", encoding="utf-8")

    qname = xh._detect_root_qname_bytes(has_decl)
    assert qname == b"rows", "Root qname should be detected when XML is valid"

    none_qname = xh._detect_root_qname_bytes(no_xml)
    assert none_qname is None, "Should return None when no start tag exists"


def test_closing_tag_bytes_for_known_and_fallback(tmp_path: Path) -> None:
    known = tmp_path / "a.xml"
    known.write_text("<root><x /></root>", encoding="utf-8")
    got_known = xh._closing_tag_bytes_for(known, "ignored")
    assert got_known == b"</root>", "Uses detected QName if present"

    unknown = tmp_path / "b.xml"
    unknown.write_text("no xml here", encoding="utf-8")
    got_fallback = xh._closing_tag_bytes_for(unknown, "rows")
    assert got_fallback == b"</rows>", "Falls back to provided root name"


def test_exclusive_lock_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(xh.os, "name", "nt")

    class _FakeMSVCRT:
        LK_LOCK = 1
        LK_UNLCK = 2

        def __init__(self) -> None:
            self.calls = 0

        def locking(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls in (1, 3):
                raise OSError("simulate busy")

    fake = _FakeMSVCRT()
    monkeypatch.setitem(sys.modules, "msvcrt", fake)  # type: ignore[arg-type]

    dummy = _DummyFile()
    with xh._exclusive_lock(dummy):
        pass


def test_exclusive_lock_posix_branch_covers_excepts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(xh.os, "name", "posix")

    class _FakeFCNTL(types.SimpleNamespace):
        LOCK_EX = 1
        LOCK_UN = 2

        def flock(self, *_args, **_kwargs):
            raise RuntimeError("no flock here")

    fake = _FakeFCNTL()
    monkeypatch.setitem(sys.modules, "fcntl", fake)  # type: ignore[arg-type]

    dummy = _DummyFile()
    with xh._exclusive_lock(dummy):
        pass


def test_write_xml_bulk_uses_exact_decl_and_handles_empty_df(tmp_path: Path) -> None:
    p = tmp_path / "rows.xml"
    import pandas as pd

    xh.write_xml_bulk(p, pd.DataFrame([]), root_tag="rows", record_tag="row")
    txt = p.read_text(encoding="utf-8")

    norm = _canonical_xml(txt)

    assert norm.startswith("<?xml version='1.0' encoding='utf-8'?>\n")
    assert norm.rstrip().endswith("<rows/>") or norm.rstrip().endswith("<rows />")
