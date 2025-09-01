from typing import Iterable, Tuple


def mapping_rules_iter(
    *pairs: Tuple[str, str, str, str]
) -> Iterable[Tuple[str, str, str, str]]:
    """Yield rules in (src_port, src_path, dst_port, dst_path) format."""
    for r in pairs:
        yield r
