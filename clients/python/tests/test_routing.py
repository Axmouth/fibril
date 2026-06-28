from fibril.routing import _TopicGlob


def test_topic_glob_matches_prefix_suffix_and_middle() -> None:
    assert _TopicGlob("events.*").matches("events.click")
    assert _TopicGlob("events.*").matches("events.")
    assert not _TopicGlob("events.*").matches("orders.new")

    assert _TopicGlob("*.dead").matches("orders.dead")
    assert not _TopicGlob("*.dead").matches("orders.live")

    assert _TopicGlob("a*c").matches("abc")
    assert _TopicGlob("a*c").matches("ac")
    assert not _TopicGlob("a*c").matches("ab")

    # No wildcard is an exact match; "*" matches everything.
    assert _TopicGlob("orders").matches("orders")
    assert not _TopicGlob("orders").matches("orders.new")
    assert _TopicGlob("*").matches("anything.at.all")
