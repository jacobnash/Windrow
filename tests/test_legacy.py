"""Pre-rename (FBF) installs keep working: env fallback and state-file adoption."""

from windrow import legacy


def test_env_prefers_new_name_then_old(monkeypatch):
    monkeypatch.delenv("WINDROW_SITE_ID", raising=False)
    monkeypatch.delenv("FBF_SITE_ID", raising=False)
    assert legacy.env("WINDROW_SITE_ID", "dflt") == "dflt"
    monkeypatch.setenv("FBF_SITE_ID", "old")
    assert legacy.env("WINDROW_SITE_ID") == "old"
    monkeypatch.setenv("WINDROW_SITE_ID", "new")
    assert legacy.env("WINDROW_SITE_ID") == "new"


def test_adopts_old_state_file_once(tmp_path):
    old = tmp_path / "fbf-connections.json"
    old.write_text('[{"id": "c1"}]')
    new = tmp_path / "windrow-connections.json"
    assert legacy.adopt_state_file(str(new)) == str(new)
    assert new.read_text() == '[{"id": "c1"}]' and not old.exists()


def test_never_overwrites_existing_new_file(tmp_path):
    (tmp_path / "fbf-devices.json").write_text("old")
    new = tmp_path / "windrow-devices.json"
    new.write_text("new")
    legacy.adopt_state_file(str(new))
    assert new.read_text() == "new" and (tmp_path / "fbf-devices.json").read_text() == "old"


def test_ignores_non_default_names(tmp_path):
    (tmp_path / "fbf-x.json").write_text("old")
    custom = tmp_path / "my-state.json"
    assert legacy.adopt_state_file(str(custom)) == str(custom)
    assert not custom.exists()


def test_credentials_key_falls_back_to_fbf_name(monkeypatch):
    from cryptography.fernet import Fernet

    from windrow import credentials

    monkeypatch.delenv("WINDROW_CREDENTIAL_KEY", raising=False)
    monkeypatch.setenv("FBF_CREDENTIAL_KEY", Fernet.generate_key().decode())
    rec = credentials.encrypt("u", "p")
    assert credentials.decrypt(rec) == "p" and rec["username"] == "u"
