"""haystack_source discovery follows a server's read paging (Baler#280).

Baler pages reads at 1,000 records: a short page's meta carries `after`,
and a client that doesn't send it back only ever sees the first page.
Fake clients stand in for openhay's: one with `read_page`, one with only
`call` (an openhay from before read_page), and a server that doesn't page
at all (Haxall), which must still be one plain read."""

import pytest

oh = pytest.importorskip("openhay")

from windrow import haystack_source

TOTAL = 2_500
PAGE = 1_000


def _recs(n=TOTAL):
    return [{"id": oh.Ref(f"p{i:05d}"), "point": oh.MARKER, "dis": f"Point {i}"} for i in range(n)]


class PagingServer:
    """Keyset pages in id order, like Baler: meta `after` = last id of a
    full page; the page after the last one has no `after`."""

    def __init__(self, recs, page=PAGE, repeat_cursor=False):
        self.recs = recs
        self.page = page
        self.repeat_cursor = repeat_cursor
        self.requests = []

    def answer(self, filter, after):
        self.requests.append((filter, after))
        start = 0
        if after is not None and not self.repeat_cursor:
            key = after.id if isinstance(after, oh.Ref) else str(after).lstrip("@")
            start = next(i for i, r in enumerate(self.recs) if r["id"].id == key) + 1
        rows = self.recs[start:start + self.page]
        meta = {}
        if start + self.page < len(self.recs):
            meta = {"incomplete": f"limit of {self.page} reached; pass after to read on", "limit": oh.Number(self.page),
                    "after": rows[-1]["id"]}
        return oh.Grid.from_rows(rows, meta=meta)


class ReadPageClient:
    """openhay with Client.read_page(filter, limit, after)."""

    def __init__(self, server):
        self.server = server

    def read_page(self, filter, limit=None, after=None):
        return self.server.answer(filter, after)

    def read(self, filter, limit=None):
        raise AssertionError("discovery must not use the one-page read")


class CallOnlyClient:
    """openhay before read_page: page through call("read") with `after` in
    the request grid."""

    def __init__(self, server):
        self.server = server

    def call(self, op, req):
        assert op == "read"
        (row,) = req.rows
        return self.server.answer(row["filter"], row.get("after"))

    def read(self, filter, limit=None):
        raise AssertionError("discovery must not use the one-page read")


@pytest.mark.parametrize("client_cls", [ReadPageClient, CallOnlyClient])
def test_2500_points_across_pages_are_all_found(client_cls):
    server = PagingServer(_recs())
    recs = haystack_source.read_all(client_cls(server), "point")
    assert len(recs) == TOTAL
    assert [r["id"].id for r in recs] == [f"p{i:05d}" for i in range(TOTAL)]
    assert [after.id if after else None for _, after in server.requests] == [None, "p00999", "p01999"]
    assert all(f == "point" for f, _ in server.requests)


def test_a_server_that_doesnt_page_is_one_plain_read():
    # Haxall/SkySpark: everything in one grid, no `after`, and no `after`
    # sent - the request is a standard Haystack read
    server = PagingServer(_recs(), page=10**9)
    recs = haystack_source.read_all(CallOnlyClient(server), "point")
    assert len(recs) == TOTAL and server.requests == [("point", None)]


def test_a_page_ending_exactly_on_the_last_match_ends_the_walk():
    # Baler: the page after one that ended on the last record comes back empty, without `after`
    server = PagingServer(_recs(2_000))
    assert len(haystack_source.read_all(ReadPageClient(server), "point")) == 2_000
    assert len(server.requests) == 2


def test_a_server_repeating_its_cursor_doesnt_loop_forever():
    server = PagingServer(_recs(), repeat_cursor=True)
    recs = haystack_source.read_all(CallOnlyClient(server), "point")
    assert len(recs) == PAGE  # duplicates dropped, then stopped
    assert len(server.requests) <= 3


def test_discover_uses_every_page():
    class Mqtt:
        def is_connected(self):
            return True

        def publish(self, *a, **k):
            pass

    server = PagingServer(_recs())
    client = CallOnlyClient(server)
    client.read_ids = lambda ids: [None for _ in ids]
    source = haystack_source.HaystackSource(client, Mqtt(), "big-site")
    assert source.discover() == TOTAL
