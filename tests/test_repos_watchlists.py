"""Tests for WatchlistRepository (src/database/repos/watchlists.py)."""


def test_create_watchlist(tmp_db):
    """Create, verify returned ID."""
    wl_id = tmp_db.watchlists.create_watchlist("My Watchlist", "NSE_EQ")
    assert wl_id is not None
    assert isinstance(wl_id, int)


def test_get_watchlists(tmp_db):
    """Create 2 watchlists, verify list."""
    tmp_db.watchlists.create_watchlist("Watchlist A")
    tmp_db.watchlists.create_watchlist("Watchlist B")

    watchlists = tmp_db.watchlists.get_watchlists()
    assert len(watchlists) == 2
    names = {wl["name"] for wl in watchlists}
    assert "Watchlist A" in names
    assert "Watchlist B" in names


def test_add_items_to_watchlist(tmp_db):
    """Create watchlist, add items, verify count."""
    wl_id = tmp_db.watchlists.create_watchlist("Items Test")
    keys = ["NSE_EQ|RELIANCE", "NSE_EQ|TCS", "NSE_EQ|INFY"]

    count = tmp_db.watchlists.add_to_watchlist(wl_id, keys)
    assert count == 3

    # Verify via get_watchlists item_count
    watchlists = tmp_db.watchlists.get_watchlists()
    wl = next(w for w in watchlists if w["name"] == "Items Test")
    assert wl["item_count"] == 3


def test_remove_item_from_watchlist(tmp_db):
    """Add items, remove one, verify."""
    wl_id = tmp_db.watchlists.create_watchlist("Remove Test")
    tmp_db.watchlists.add_to_watchlist(wl_id, ["NSE_EQ|RELIANCE", "NSE_EQ|TCS"])

    tmp_db.watchlists.remove_from_watchlist(wl_id, "NSE_EQ|RELIANCE")

    watchlists = tmp_db.watchlists.get_watchlists()
    wl = next(w for w in watchlists if w["name"] == "Remove Test")
    assert wl["item_count"] == 1


def test_delete_watchlist(tmp_db):
    """Create, delete, verify gone."""
    wl_id = tmp_db.watchlists.create_watchlist("Delete Me")
    tmp_db.watchlists.add_to_watchlist(wl_id, ["NSE_EQ|RELIANCE"])

    tmp_db.watchlists.delete_watchlist(wl_id)

    watchlists = tmp_db.watchlists.get_watchlists()
    assert all(wl["name"] != "Delete Me" for wl in watchlists)
