import pytest
import threading
import time

from etl_core.context.secrets.memory_provider import InMemorySecretProvider


class TestInMemorySecretProvider:
    """Test cases for InMemorySecretProvider class."""

    def setup_method(self):
        """Set up a fresh provider for each test."""
        self.provider = InMemorySecretProvider()

    def test_initialization(self):
        """Test provider initialization."""
        assert self.provider._store == {}
        assert hasattr(self.provider._lock, "acquire") and hasattr(
            self.provider._lock, "release"
        )

    def test_set_and_get_secret(self):
        """Test setting and getting a secret."""
        self.provider.set("test_key", "test_secret")
        result = self.provider.get("test_key")
        assert result == "test_secret"

    def test_get_nonexistent_secret(self):
        """Test getting a non-existent secret raises KeyError."""
        with pytest.raises(KeyError, match="Secret not found for key='nonexistent'"):
            self.provider.get("nonexistent")

    def test_exists_true(self):
        """Test exists returns True for existing secret."""
        self.provider.set("test_key", "test_secret")
        assert self.provider.exists("test_key") is True

    def test_exists_false(self):
        """Test exists returns False for non-existent secret."""
        assert self.provider.exists("nonexistent") is False

    def test_delete_existing_secret(self):
        """Test deleting an existing secret."""
        self.provider.set("test_key", "test_secret")
        assert self.provider.exists("test_key") is True

        self.provider.delete("test_key")
        assert self.provider.exists("test_key") is False

    def test_delete_nonexistent_secret(self):
        """Test deleting a non-existent secret (should not raise error)."""
        self.provider.delete("nonexistent")
        assert self.provider.exists("nonexistent") is False

    def test_overwrite_secret(self):
        """Test overwriting an existing secret."""
        self.provider.set("test_key", "old_secret")
        assert self.provider.get("test_key") == "old_secret"

        self.provider.set("test_key", "new_secret")
        assert self.provider.get("test_key") == "new_secret"

    def test_multiple_secrets(self):
        """Test handling multiple secrets."""
        secrets = {"key1": "value1", "key2": "value2", "key3": "value3"}

        for key, value in secrets.items():
            self.provider.set(key, value)

        for key, value in secrets.items():
            assert self.provider.get(key) == value
            assert self.provider.exists(key) is True

    def test_empty_string_secret(self):
        """Test handling empty string secrets."""
        self.provider.set("empty_key", "")
        assert self.provider.get("empty_key") == ""
        assert self.provider.exists("empty_key") is True

    def test_none_secret(self):
        """Test handling None secrets."""
        self.provider.set("none_key", None)
        assert self.provider.get("none_key") is None
        assert self.provider.exists("none_key") is True

    def test_special_characters_in_key(self):
        """Test handling special characters in keys."""
        special_key = "key/with/special@chars!#$%"
        self.provider.set(special_key, "special_value")
        assert self.provider.get(special_key) == "special_value"
        assert self.provider.exists(special_key) is True

    def test_special_characters_in_value(self):
        """Test handling special characters in values."""
        special_value = "value with spaces, tabs\t, newlines\n, and unicode: 🚀"
        self.provider.set("test_key", special_value)
        assert self.provider.get("test_key") == special_value

    def test_long_key_and_value(self):
        """Test handling very long keys and values."""
        long_key = "a" * 1000
        long_value = "b" * 10000

        self.provider.set(long_key, long_value)
        assert self.provider.get(long_key) == long_value
        assert self.provider.exists(long_key) is True

    def test_thread_safety_single_provider(self):
        """Test thread safety with multiple threads accessing the same provider."""
        results = []
        errors = []

        def worker(thread_id):
            try:
                key = f"thread_{thread_id}_key"
                value = f"thread_{thread_id}_value"
                self.provider.set(key, value)

                time.sleep(0.001)

                retrieved = self.provider.get(key)
                if retrieved == value:
                    results.append(True)
                else:
                    results.append(False)

            except Exception as e:
                errors.append(str(e))

        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(results) == 10
        assert all(results)
        assert len(errors) == 0

    def test_thread_safety_multiple_providers(self):
        """Test thread safety with multiple providers in different threads."""
        results = []
        errors = []

        def worker(thread_id):
            try:
                provider = InMemorySecretProvider()
                key = f"thread_{thread_id}_key"
                value = f"thread_{thread_id}_value"

                provider.set(key, value)
                retrieved = provider.get(key)

                if retrieved == value:
                    results.append(True)
                else:
                    results.append(False)

            except Exception as e:
                errors.append(str(e))

        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(results) == 10
        assert all(results)
        assert len(errors) == 0

    def test_lock_reentrancy(self):
        """Test that the RLock allows reentrant access."""

        def nested_operations():
            self.provider.set("key1", "value1")

            self.provider.set("key2", "value2")

            assert self.provider.get("key1") == "value1"
            assert self.provider.get("key2") == "value2"

        nested_operations()

        assert self.provider.get("key1") == "value1"
        assert self.provider.get("key2") == "value2"

    def test_memory_isolation_between_instances(self):
        """Test that different provider instances don't share memory."""
        provider1 = InMemorySecretProvider()
        provider2 = InMemorySecretProvider()

        provider1.set("shared_key", "provider1_value")

        assert not provider2.exists("shared_key")

        provider2.set("shared_key", "provider2_value")

        assert provider1.get("shared_key") == "provider1_value"
        assert provider2.get("shared_key") == "provider2_value"

    def test_delete_after_get(self):
        """Test deleting a secret after getting it."""
        self.provider.set("test_key", "test_value")

        value = self.provider.get("test_key")
        assert value == "test_value"

        self.provider.delete("test_key")

        assert not self.provider.exists("test_key")
        with pytest.raises(KeyError):
            self.provider.get("test_key")

    def test_set_get_delete_cycle(self):
        """Test complete set-get-delete cycle."""
        self.provider.set("cycle_key", "cycle_value")
        assert self.provider.exists("cycle_key") is True

        value = self.provider.get("cycle_key")
        assert value == "cycle_value"

        self.provider.delete("cycle_key")
        assert self.provider.exists("cycle_key") is False

        self.provider.set("cycle_key", "new_value")
        assert self.provider.exists("cycle_key") is True
        assert self.provider.get("cycle_key") == "new_value"

    def test_unicode_support(self):
        """Test handling of Unicode characters."""
        unicode_key = "ключ_с_кириллицей"
        unicode_value = "значение с кириллицей и эмодзи 🎉"

        self.provider.set(unicode_key, unicode_value)
        assert self.provider.get(unicode_key) == unicode_value
        assert self.provider.exists(unicode_key) is True

        self.provider.delete(unicode_key)
        assert not self.provider.exists(unicode_key)
