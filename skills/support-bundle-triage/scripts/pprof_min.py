"""Minimal pprof (perftools profile.proto) reader: no protobuf dependency.

Only the fields needed for cumulative-per-function attribution are decoded.
Schema reference (github.com/google/pprof/proto/profile.proto):
  Profile: sample_type=1 sample=2 mapping=3 location=4 function=5 string_table=6
  ValueType: type=1 unit=2                 (string_table indices)
  Sample: location_id=1 (packed) value=2 (packed) label=3
  Location: id=1 mapping_id=2 address=3 line=4
  Line: function_id=1 line=2
  Function: id=1 name=2 system_name=3 filename=4 start_line=5
"""
import gzip
import re


def _varint(b, i):
    shift = 0
    val = 0
    while True:
        x = b[i]
        i += 1
        val |= (x & 0x7F) << shift
        if not x & 0x80:
            return val, i
        shift += 7


def _fields(b, start=0, end=None):
    """Yield (field_number, wire_type, payload) for one message body."""
    if end is None:
        end = len(b)
    i = start
    while i < end:
        key, i = _varint(b, i)
        fn, wt = key >> 3, key & 7
        if wt == 0:
            v, i = _varint(b, i)
            yield fn, wt, v
        elif wt == 2:
            ln, i = _varint(b, i)
            yield fn, wt, b[i:i + ln]
            i += ln
        elif wt == 5:
            yield fn, wt, b[i:i + 4]
            i += 4
        elif wt == 1:
            yield fn, wt, b[i:i + 8]
            i += 8
        else:
            raise ValueError(f"unsupported wire type {wt} at {i}")


def _packed_varints(buf):
    out = []
    i = 0
    while i < len(buf):
        v, i = _varint(buf, i)
        out.append(v)
    return out


def parse_pprof(path):
    """-> (sample_types, samples, ) where
    sample_types = [(type_name, unit_name), ...]
    samples      = [(frozenset_of_function_names, [values...]), ...]
    Frames are ordered leaf-first, as pprof stores them.
    """
    raw = gzip.open(path, "rb").read() if path.endswith(".gz") else open(path, "rb").read()

    strings = []
    functions = {}          # id -> name index
    locations = {}          # id -> [function ids]
    sample_types = []
    raw_samples = []

    for fn, _wt, payload in _fields(raw):
        if fn == 6:
            strings.append(payload.decode("utf-8", "replace"))
        elif fn == 1:
            t = u = 0
            for sfn, _w, v in _fields(payload):
                if sfn == 1:
                    t = v
                elif sfn == 2:
                    u = v
            sample_types.append((t, u))
        elif fn == 5:
            fid = name = 0
            for sfn, _w, v in _fields(payload):
                if sfn == 1:
                    fid = v
                elif sfn == 2:
                    name = v
            functions[fid] = name
        elif fn == 4:
            lid = 0
            fids = []
            for sfn, w, v in _fields(payload):
                if sfn == 1:
                    lid = v
                elif sfn == 4 and w == 2:
                    for lfn, _w2, lv in _fields(v):
                        if lfn == 1:
                            fids.append(lv)
            locations[lid] = fids
        elif fn == 2:
            locs = []
            vals = []
            for sfn, w, v in _fields(payload):
                if sfn == 1:
                    locs = _packed_varints(v) if w == 2 else [v]
                elif sfn == 2:
                    vals = _packed_varints(v) if w == 2 else [v]
            raw_samples.append((locs, vals))

    def sname(i):
        return strings[i] if 0 <= i < len(strings) else ""

    types = [(sname(t), sname(u)) for t, u in sample_types]
    samples = []
    for locs, vals in raw_samples:
        names = []
        for lid in locs:
            for fid in locations.get(lid, ()):
                names.append(sname(functions.get(fid, 0)))
        samples.append((names, vals))
    return types, samples


def cumulative(samples, value_index):
    """-> {function name: cumulative value}. A function is counted once per
    sample even if it appears several times in the stack, which is what
    pprof's 'cum' column means."""
    out = {}
    for names, vals in samples:
        if value_index >= len(vals):
            continue
        v = vals[value_index]
        for n in set(names):
            out[n] = out.get(n, 0) + v
    return out


def total(samples, value_index):
    return sum(v[value_index] for _n, v in samples if value_index < len(v))


# Feldera heap profiles come from jemalloc_pprof. Every stack begins with the
# allocator's own profiling shim, so pprof's "flat" column attributes 100% to
# prof_backtrace_impl and is useless. The first frame past the shim is the real
# allocation site.
SHIM = re.compile(
    r"^(prof_backtrace_impl|_rjem_je_\w+|je_\w+|do_rallocx|do_ralloc\w*"
    r"|iralloct\w*|arena_\w*|_?malloc\w*|_?realloc\w*|_?calloc\w*)"
)


def alloc_site(frames):
    """First frame past the allocator shim: the real allocation site."""
    for f in frames:
        if not SHIM.match(f):
            return f
    return "(all shim)"


def by_alloc_site(samples, value_index=0):
    """-> {site: bytes}, an exclusive partition that sums to the profile total."""
    out = {}
    for frames, vals in samples:
        if value_index >= len(vals):
            continue
        s = alloc_site(frames)
        out[s] = out.get(s, 0) + vals[value_index]
    return out


OPERATOR_HASH = re.compile(r"feldera_pipe_operator_([0-9a-f]+)")


def by_operator_hash(samples, value_index=0):
    """-> {16-hex operator hash: bytes}.

    Generated operator symbols embed the first 16 hex of the MIR node's
    persistent_id, which joins to dataflow_graph.json and thus to a table,
    view, and SQL line. A sample is counted once per distinct hash.
    """
    out = {}
    for frames, vals in samples:
        if value_index >= len(vals):
            continue
        hashes = {m for f in frames for m in OPERATOR_HASH.findall(f)}
        for h in hashes:
            out[h] = out.get(h, 0) + vals[value_index]
    return out
