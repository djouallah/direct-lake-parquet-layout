"""The footer `encoding_stats` decoder in stats.py — offline, no store, no pyarrow.

The bytes are built here with a minimal Thrift compact WRITER, so the test pins the parser against
the parquet-format field ids rather than against whatever one library happens to emit. It was also
checked by hand against real files (pyarrow with and without dictionaries, DuckDB 1.5.5): the
decoder read pyarrow's dictionary chunks as declared and fully dictionary, its PLAIN chunks as
declared and not, and DuckDB 1.5.5's as undeclared — the pre-duckdb#24957 writer.
"""
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(__file__))


@pytest.fixture(scope="module")
def stats():
    """`stats.py` with its Fabric-facing imports stubbed, exactly as test_ordering.py does."""
    duckrun = sys.modules.setdefault("duckrun", types.ModuleType("duckrun"))
    auth = sys.modules.setdefault("duckrun.auth", types.ModuleType("duckrun.auth"))
    auth.get_fabric_token = lambda *a, **k: "stub-token"
    duckrun.auth = auth
    sys.modules.setdefault("requests", types.ModuleType("requests"))
    os.environ.setdefault("WS_ID", "00000000-0000-0000-0000-000000000000")
    import stats as mod
    return mod


def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _zz(n):
    return _varint((n << 1) ^ (n >> 63))


def _field(fid, last, t):
    d = fid - last
    return bytes([(d << 4) | t]) if 0 < d <= 15 else bytes([t]) + _zz(fid)


def _struct(fields):
    """`fields` is [(fid, type, encoded value bytes)] in ascending fid order."""
    out, last = bytearray(), 0
    for fid, t, v in fields:
        out += _field(fid, last, t) + v
        last = fid
    return bytes(out) + b"\x00"


def _list(et, items):
    n = len(items)
    head = bytes([(n << 4) | et]) if n < 15 else bytes([0xF0 | et]) + _varint(n)
    return head + b"".join(items)


def _i32(n):
    return _zz(n)


def _bin(s):
    b = s.encode()
    return _varint(len(b)) + b


def _chunk(path, encoding_stats=None, extra_before=True):
    md = [(1, 5, _i32(1)),                                         # type
          (2, 9, _list(5, [_i32(0), _i32(8)])),                    # encodings
          (3, 9, _list(8, [_bin(path)])),                          # path_in_schema
          (4, 5, _i32(1)),                                         # codec
          (5, 6, _zz(100))]                                        # num_values
    if extra_before:
        md.append((12, 12, _struct([(1, 8, _bin("max")), (2, 8, _bin("min"))])))  # statistics
    if encoding_stats is not None:
        md.append((13, 9, _list(12, [_struct([(1, 5, _i32(pt)), (2, 5, _i32(enc)), (3, 5, _i32(cnt))])
                                     for pt, enc, cnt in encoding_stats])))
    return _struct([(2, 6, _zz(4)), (3, 12, _struct(md))])


def _footer(row_groups):
    rgs = [_struct([(1, 9, _list(12, chunks)), (2, 6, _zz(1000)), (3, 6, _zz(10))]) for chunks in row_groups]
    return _struct([(1, 5, _i32(2)), (3, 6, _zz(10)), (4, 9, _list(12, rgs))])


DICT_ONLY = [(2, 0, 1), (0, 8, 5)]          # dictionary page PLAIN, data pages RLE_DICTIONARY
FELL_BACK = [(2, 0, 1), (0, 8, 2), (0, 0, 3)]  # some data pages fell back to PLAIN
NO_DICT = [(0, 0, 4)]                        # PLAIN data pages, no dictionary page
V1_DICT = [(2, 2, 1), (0, 2, 4)]            # parquet-mr v1: PLAIN_DICTIONARY on both


def test_a_declared_fully_dictionary_chunk_is_remap_eligible(stats):
    out = stats.footer_encoding_stats(_footer([[_chunk("price", DICT_ONLY)]]))
    assert out == {"price": [{"declared": True, "dict_only": True}]}


def test_a_chunk_whose_pages_fell_back_to_plain_is_not(stats):
    out = stats.footer_encoding_stats(_footer([[_chunk("mw", FELL_BACK)]]))
    assert out["mw"] == [{"declared": True, "dict_only": False}]


def test_no_dictionary_page_is_not_dictionary_even_if_declared(stats):
    out = stats.footer_encoding_stats(_footer([[_chunk("mw", NO_DICT)]]))
    assert out["mw"] == [{"declared": True, "dict_only": False}]


def test_legacy_plain_dictionary_counts_as_dictionary(stats):
    out = stats.footer_encoding_stats(_footer([[_chunk("DUID", V1_DICT)]]))
    assert out["DUID"] == [{"declared": True, "dict_only": True}]


def test_a_footer_without_encoding_stats_is_undeclared_not_plain(stats):
    """What DuckDB wrote before duckdb#24957: a dictionary page may exist, nothing says so."""
    out = stats.footer_encoding_stats(_footer([[_chunk("date", None)]]))
    assert out["date"] == [{"declared": False, "dict_only": False}]


def test_chunks_aggregate_across_row_groups_and_columns(stats):
    out = stats.footer_encoding_stats(_footer([
        [_chunk("a", DICT_ONLY), _chunk("b", None, extra_before=False)],
        [_chunk("a", FELL_BACK), _chunk("b", DICT_ONLY)],
    ]))
    assert [c["dict_only"] for c in out["a"]] == [True, False]
    assert [c["declared"] for c in out["b"]] == [False, True]


def test_a_long_list_header_and_large_field_delta_parse(stats):
    """15+ chunks forces the long list form; field 13 after field 5 is a delta of 8, after 3 of 10."""
    out = stats.footer_encoding_stats(_footer([[_chunk(f"c{i}", DICT_ONLY, extra_before=False)
                                                for i in range(20)]]))
    assert len(out) == 20 and all(v[0]["dict_only"] for v in out.values())


def test_merge_keeps_unmeasured_columns_free_of_counts(stats):
    enc = {"a": {"chunks": 2}, "b": {"chunks": 2}}
    stats.merge_declarations(enc, {"a": {"declared": 2, "dict_declared": 1}, "zzz": {"declared": 9}})
    assert enc == {"a": {"chunks": 2, "declared": 2, "dict_declared": 1}, "b": {"chunks": 2}}
