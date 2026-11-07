import json
import random

from windrow import mock_weather


def test_values_stay_physical_over_a_day():
    rng = random.Random(1)
    for h in range(0, 24 * 3600, 900):
        hum = mock_weather.value_at("humidity", h, rng)
        assert 5 <= hum <= 100
        assert mock_weather.value_at("wind-speed", h, rng) >= 0
        assert 0 <= mock_weather.value_at("wind-direction", h, rng) < 360


class Recorder:
    def __init__(self):
        self.sent = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.sent.append((topic, json.loads(payload), retain))


def test_tags_use_baler_listener_envelope():
    c = Recorder()
    mock_weather.publish_tags(c, "fbf/home/weather", "Home weather")
    topics = {t: p for t, p, _r in c.sent}
    equip = json.loads(topics["fbf/home/weather/equip/tags"]["value"])
    assert equip["tags"]["weatherStation"] is True and set(equip["points"]) == set(mock_weather.POINTS)
    assert json.loads(topics["fbf/home/weather/temp/tags"]["value"])["unit"] == "°F"
    assert all(r for _t, _p, r in c.sent)  # retained
