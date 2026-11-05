from windrow.connection_manager import should_publish


def test_no_policy_configured_always_publishes():
    assert should_publish(71.0, 71.0, last_published_at=100.0, now=100.1) is True


def test_first_ever_reading_always_publishes_even_with_cov_configured():
    assert should_publish(71.0, None, last_published_at=None, now=100.0, his_collect_cov=0.5) is True


def test_cov_numeric_within_threshold_suppressed():
    assert should_publish(71.2, 71.0, last_published_at=100.0, now=100.1, his_collect_cov=0.5) is False


def test_cov_numeric_beyond_threshold_publishes():
    assert should_publish(71.6, 71.0, last_published_at=100.0, now=100.1, his_collect_cov=0.5) is True


def test_cov_marker_any_change_publishes_even_tiny_delta():
    assert should_publish(71.001, 71.0, last_published_at=100.0, now=100.1, his_collect_cov=True) is True


def test_cov_marker_no_change_suppressed():
    assert should_publish(71.0, 71.0, last_published_at=100.0, now=100.1, his_collect_cov=True) is False


def test_cov_non_numeric_equality_change_publishes():
    assert should_publish("active", "inactive", last_published_at=100.0, now=100.1, his_collect_cov=0.5) is True


def test_cov_non_numeric_equality_no_change_suppressed():
    assert should_publish("active", "active", last_published_at=100.0, now=100.1, his_collect_cov=0.5) is False


def test_cov_rate_limit_throttles_even_a_real_change():
    # changed enough (10 > 0.5), but only 0.1s elapsed and rate limit is 5s
    assert (
        should_publish(81.0, 71.0, last_published_at=100.0, now=100.1, his_collect_cov=0.5, his_collect_cov_rate_limit=5.0)
        is False
    )


def test_cov_rate_limit_allows_change_once_elapsed():
    assert (
        should_publish(81.0, 71.0, last_published_at=100.0, now=106.0, his_collect_cov=0.5, his_collect_cov_rate_limit=5.0)
        is True
    )


def test_interval_mode_suppressed_before_interval_elapses():
    assert should_publish(999.0, 71.0, last_published_at=100.0, now=101.0, his_collect_interval=5.0) is False


def test_interval_mode_publishes_once_interval_elapses_regardless_of_value():
    # value didn't even change, but interval mode publishes anyway on schedule
    assert should_publish(71.0, 71.0, last_published_at=100.0, now=105.0, his_collect_interval=5.0) is True
