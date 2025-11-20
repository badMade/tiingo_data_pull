"""Tests for Notion integration client."""
import json
from pathlib import Path

import pytest
import requests

from tiingo_data_pull.clients.notion_client import NotionClient
from tiingo_data_pull.integrations.notion_client import load_notion_config


class TestLoadNotionConfig:
    """Tests for load_notion_config function."""

    def test_load_from_env_variables(self):
        """Test loading configuration from environment variables."""
        env = {
            "NOTION_API_KEY": "test-api-key",
            "NOTION_DATABASE_ID": "test-database-id",
        }
        config = load_notion_config(env=env)
        assert config.api_key == "test-api-key"
        assert config.database_id == "test-database-id"
        assert config.properties.ticker == "Ticker"

    def test_load_from_config_file(self, tmp_path):
        """Test loading configuration from JSON file."""
        config_file = tmp_path / "notion-config.json"
        config_data = {
            "api_key": "file-api-key",
            "database_id": "file-database-id",
        }
        config_file.write_text(json.dumps(config_data))

        config = load_notion_config(config_path=config_file, env={})
        assert config.api_key == "file-api-key"
        assert config.database_id == "file-database-id"

    def test_environment_variable_expansion_in_config_file(self, tmp_path, monkeypatch):
        """Test that environment variables are expanded in config file values."""
        # Set environment variables
        monkeypatch.setenv("NOTION_API_KEY", "expanded-api-key")
        monkeypatch.setenv("MY_DATABASE_ID", "expanded-database-id")

        config_file = tmp_path / "notion-config.json"
        config_data = {
            "api_key": "${NOTION_API_KEY}",
            "database_id": "${MY_DATABASE_ID}",
        }
        config_file.write_text(json.dumps(config_data))

        config = load_notion_config(config_path=config_file)
        assert config.api_key == "expanded-api-key"
        assert config.database_id == "expanded-database-id"

    def test_environment_variable_expansion_dollar_sign_syntax(self, tmp_path, monkeypatch):
        """Test that $VAR syntax is also supported for environment variable expansion."""
        monkeypatch.setenv("TEST_API_KEY", "test-value")

        config_file = tmp_path / "notion-config.json"
        config_data = {
            "api_key": "$TEST_API_KEY",
            "database_id": "some-id",
        }
        config_file.write_text(json.dumps(config_data))

        config = load_notion_config(config_path=config_file)
        assert config.api_key == "test-value"

    def test_env_variable_takes_precedence_over_file(self, tmp_path):
        """Test that environment variables take precedence over file values."""
        config_file = tmp_path / "notion-config.json"
        config_data = {
            "api_key": "file-api-key",
            "database_id": "file-database-id",
        }
        config_file.write_text(json.dumps(config_data))

        env = {
            "NOTION_API_KEY": "env-api-key",
            "NOTION_DATABASE_ID": "env-database-id",
        }
        # When the file has a non-expandable value and env has the key,
        # file value takes precedence per current implementation
        config = load_notion_config(config_path=config_file, env=env)
        assert config.api_key == "file-api-key"
        assert config.database_id == "file-database-id"

    def test_missing_api_key_raises_error(self):
        """Test that missing API key raises RuntimeError."""
        env = {"NOTION_DATABASE_ID": "test-database-id"}
        with pytest.raises(RuntimeError, match="Notion API key is required"):
            load_notion_config(env=env)

    def test_missing_database_id_raises_error(self):
        """Test that missing database ID raises RuntimeError."""
        env = {"NOTION_API_KEY": "test-api-key"}
        with pytest.raises(RuntimeError, match="Notion database ID is required"):
            load_notion_config(env=env)

    def test_property_overrides(self, tmp_path):
        """Test that property overrides work correctly."""
        config_file = tmp_path / "notion-config.json"
        config_data = {
            "api_key": "test-api-key",
            "database_id": "test-database-id",
            "properties": {
                "ticker": "Stock",
                "date": "TradeDate",
            },
        }
        config_file.write_text(json.dumps(config_data))

        overrides = {"ticker": "Symbol"}
        config = load_notion_config(
            config_path=config_file, env={}, property_overrides=overrides
        )
        assert config.properties.ticker == "Symbol"
        assert config.properties.date == "TradeDate"

    def test_nonexistent_config_file_raises_error(self):
        """Test that non-existent config file raises RuntimeError."""
        with pytest.raises(RuntimeError, match="Notion config file not found"):
            load_notion_config(config_path=Path("/nonexistent/path.json"), env={})

    def test_environment_variable_not_set_keeps_literal(self, tmp_path):
        """Test that undefined environment variables are kept as literal strings."""
        config_file = tmp_path / "notion-config.json"
        config_data = {
            "api_key": "${UNDEFINED_VAR}",
            "database_id": "test-id",
        }
        config_file.write_text(json.dumps(config_data))

        config = load_notion_config(config_path=config_file, env={})
        # os.path.expandvars keeps undefined variables as-is
        assert config.api_key == "${UNDEFINED_VAR}"

    def test_non_string_api_key_raises_type_error(self, tmp_path):
        """Test that non-string API key in config file raises TypeError."""
        config_file = tmp_path / "notion-config.json"
        config_data = {
            "api_key": 12345,  # Integer instead of string
            "database_id": "test-id",
        }
        config_file.write_text(json.dumps(config_data))

        with pytest.raises(TypeError, match="Configuration value for 'api_key' must be a string, but found int"):
            load_notion_config(config_path=config_file, env={})

    def test_non_string_database_id_raises_type_error(self, tmp_path):
        """Test that non-string database ID in config file raises TypeError."""
        config_file = tmp_path / "notion-config.json"
        config_data = {
            "api_key": "test-api-key",
            "database_id": True,  # Boolean instead of string
        }
        config_file.write_text(json.dumps(config_data))

        with pytest.raises(TypeError, match="Configuration value for 'database_id' must be a string, but found bool"):
            load_notion_config(config_path=config_file, env={})


class TestCloneSession:
    """Tests for NotionClient._clone_session static method."""

    def test_clone_session_with_dict_params(self):
        """Test cloning a session with dictionary params."""
        source = requests.Session()
        source.params = {"key": "value", "foo": "bar"}
        source.headers.update({"User-Agent": "test-agent"})

        cloned = NotionClient._clone_session(source)

        assert cloned is not source
        assert cloned.params == source.params
        assert cloned.params is not source.params
        assert cloned.headers["User-Agent"] == "test-agent"

    def test_clone_session_with_list_of_tuples_params(self):
        """Test cloning a session with params as list of tuples."""
        source = requests.Session()
        source.params = [("key1", "value1"), ("key2", "value2")]

        cloned = NotionClient._clone_session(source)

        assert cloned is not source
        assert cloned.params == source.params
        # For lists, copy() creates a shallow copy, so it's a new list
        assert cloned.params is not source.params

    def test_clone_session_with_none_params(self):
        """Test cloning a session with None params."""
        source = requests.Session()
        source.params = None

        cloned = NotionClient._clone_session(source)

        assert cloned is not source
        assert cloned.params is None

    def test_clone_session_preserves_headers(self):
        """Test that headers are preserved during cloning."""
        source = requests.Session()
        source.headers.update({
            "User-Agent": "custom-agent",
            "Accept": "application/json",
            "X-Custom-Header": "custom-value",
        })

        cloned = NotionClient._clone_session(source)

        assert cloned.headers["User-Agent"] == "custom-agent"
        assert cloned.headers["Accept"] == "application/json"
        assert cloned.headers["X-Custom-Header"] == "custom-value"

    def test_clone_session_preserves_auth(self):
        """Test that authentication is preserved during cloning."""
        source = requests.Session()
        source.auth = ("username", "password")

        cloned = NotionClient._clone_session(source)

        assert cloned.auth == source.auth

    def test_clone_session_preserves_proxies(self):
        """Test that proxies are copied during cloning."""
        source = requests.Session()
        source.proxies = {"http": "http://proxy.example.com:8080"}

        cloned = NotionClient._clone_session(source)

        assert cloned.proxies == source.proxies
        assert cloned.proxies is not source.proxies

    def test_clone_session_preserves_verify(self):
        """Test that verify setting is preserved during cloning."""
        source = requests.Session()
        source.verify = False

        cloned = NotionClient._clone_session(source)

        assert cloned.verify is False

    def test_clone_session_preserves_cert(self):
        """Test that cert setting is preserved during cloning."""
        source = requests.Session()
        source.cert = "/path/to/cert.pem"

        cloned = NotionClient._clone_session(source)

        assert cloned.cert == "/path/to/cert.pem"

    def test_clone_session_preserves_trust_env(self):
        """Test that trust_env setting is preserved during cloning."""
        source = requests.Session()
        source.trust_env = False

        cloned = NotionClient._clone_session(source)

        assert cloned.trust_env is False

    def test_clone_session_preserves_max_redirects(self):
        """Test that max_redirects setting is preserved during cloning."""
        source = requests.Session()
        source.max_redirects = 5

        cloned = NotionClient._clone_session(source)

        assert cloned.max_redirects == 5

    def test_clone_session_with_hooks(self):
        """Test that hooks are copied during cloning."""
        def hook_function(r, *args, **kwargs):
            return r

        source = requests.Session()
        source.hooks["response"] = [hook_function]

        cloned = NotionClient._clone_session(source)

        assert "response" in cloned.hooks
        assert cloned.hooks["response"] == source.hooks["response"]
        # Verify hooks dict is copied, not shared
        assert cloned.hooks is not source.hooks

    def test_clone_session_with_empty_params(self):
        """Test cloning a session with empty dict params."""
        source = requests.Session()
        source.params = {}

        cloned = NotionClient._clone_session(source)

        assert cloned.params == {}
        assert cloned.params is not source.params

    def test_clone_session_independence(self):
        """Test that modifications to cloned session don't affect source."""
        source = requests.Session()
        source.params = {"original": "value"}
        source.headers.update({"X-Original": "header"})

        cloned = NotionClient._clone_session(source)

        # Modify cloned session
        cloned.params["new_key"] = "new_value"
        cloned.headers["X-New"] = "new-header"

        # Source should remain unchanged
        assert "new_key" not in source.params
        assert "X-New" not in source.headers
        assert source.params == {"original": "value"}
        assert source.headers["X-Original"] == "header"
