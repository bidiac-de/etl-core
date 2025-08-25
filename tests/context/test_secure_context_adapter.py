import pytest
from unittest.mock import Mock, patch
from dataclasses import dataclass

from src.etl_core.context.secure_context_adapter import (
    SecureContextAdapter,
    BootstrapResult
)
from src.etl_core.context.secrets.secret_provider import SecretProvider


class TestBootstrapResult:
    """Test cases for BootstrapResult dataclass."""

    def test_bootstrap_result_creation(self):
        """Test BootstrapResult creation with default values."""
        result = BootstrapResult(stored=[], skipped_existing=[], errors={})
        assert result.stored == []
        assert result.skipped_existing == []
        assert result.errors == {}

    def test_bootstrap_result_with_data(self):
        """Test BootstrapResult creation with data."""
        result = BootstrapResult(
            stored=["key1", "key2"],
            skipped_existing=["key3"],
            errors={"key4": "error message"}
        )
        assert result.stored == ["key1", "key2"]
        assert result.skipped_existing == ["key3"]
        assert result.errors == {"key4": "error message"}

    def test_bootstrap_result_immutability(self):
        """Test that BootstrapResult is immutable."""
        result = BootstrapResult(stored=[], skipped_existing=[], errors={})
        with pytest.raises(Exception):
            result.stored = ["new_value"]


class TestSecureContextAdapter:
    """Test cases for SecureContextAdapter class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_secret_store = Mock(spec=SecretProvider)
        self.provider_id = "test_provider"

    def test_initialization_with_context(self):
        """Test initialization with context."""
        mock_context = Mock()
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        assert adapter._provider_id == self.provider_id
        assert adapter._secret_store == self.mock_secret_store
        assert adapter._context == mock_context
        assert adapter._credentials is None

    def test_initialization_with_credentials(self):
        """Test initialization with credentials."""
        mock_credentials = Mock()
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            credentials=mock_credentials
        )
        
        assert adapter._provider_id == self.provider_id
        assert adapter._secret_store == self.mock_secret_store
        assert adapter._context is None
        assert adapter._credentials == mock_credentials

    def test_initialization_with_both_context_and_credentials(self):
        """Test initialization with both context and credentials."""
        mock_context = Mock()
        mock_credentials = Mock()
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context,
            credentials=mock_credentials
        )
        
        assert adapter._context == mock_context
        assert adapter._credentials == mock_credentials

    def test_initialization_without_context_or_credentials(self):
        """Test initialization without context or credentials raises error."""
        with pytest.raises(ValueError, match="Provide either `context` or `credentials`"):
            SecureContextAdapter(
                provider_id=self.provider_id,
                secret_store=self.mock_secret_store
            )

    def test_provider_id_property(self):
        """Test provider_id property."""
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=Mock()
        )
        
        assert adapter.provider_id == self.provider_id

    def test_context_property(self):
        """Test context property."""
        mock_context = Mock()
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        assert adapter.context == mock_context

    def test_credentials_property(self):
        """Test credentials property."""
        mock_credentials = Mock()
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            credentials=mock_credentials
        )
        
        assert adapter.credentials == mock_credentials

    def test_secret_key_generation(self):
        """Test _secret_key method generates correct keys."""
        adapter = SecureContextAdapter(
            provider_id="my_provider",
            secret_store=self.mock_secret_store,
            context=Mock()
        )
        
        key = adapter._secret_key("password")
        assert key == "my_provider/password"

    def test_bootstrap_to_store_context_only(self):
        """Test bootstrap_to_store with context only."""
        mock_context = Mock()
        mock_param1 = Mock()
        mock_param1.is_secure = True
        mock_param1.value = "secret_value1"
        
        mock_param2 = Mock()
        mock_param2.is_secure = False
        mock_param2.value = "plain_value"
        
        mock_param3 = Mock()
        mock_param3.is_secure = True
        mock_param3.value = "secret_value2"
        
        mock_context.parameters = {
            "param1": mock_param1,
            "param2": mock_param2,
            "param3": mock_param3
        }
        
        self.mock_secret_store.exists.return_value = False
        self.mock_secret_store.get.side_effect = ["secret_value1", "secret_value2"]
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == ["param1", "param3"]
        assert result.skipped_existing == []
        assert result.errors == {}
        
        # Verify secrets were stored
        assert self.mock_secret_store.set.call_count == 2
        self.mock_secret_store.set.assert_any_call(
            f"{self.provider_id}/param1", "secret_value1"
        )
        self.mock_secret_store.set.assert_any_call(
            f"{self.provider_id}/param3", "secret_value2"
        )
        
        # Verify context values were blanked
        assert mock_param1.value == ""
        assert mock_param2.value == "plain_value"  # Should not change
        assert mock_param3.value == ""

    def test_bootstrap_to_store_credentials_only(self):
        """Test bootstrap_to_store with credentials only."""
        mock_credentials = Mock()
        mock_credentials.decrypted_password = "secret_password"
        
        self.mock_secret_store.exists.return_value = False
        self.mock_secret_store.get.return_value = "secret_password"
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            credentials=mock_credentials
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == ["password"]
        assert result.skipped_existing == []
        assert result.errors == {}
        
        # Verify password was stored
        self.mock_secret_store.set.assert_called_once_with(
            f"{self.provider_id}/password", "secret_password"
        )

    def test_bootstrap_to_store_skip_existing(self):
        """Test bootstrap_to_store skips existing secrets."""
        mock_context = Mock()
        mock_param = Mock()
        mock_param.is_secure = True
        mock_param.value = "secret_value"
        
        mock_context.parameters = {"param": mock_param}
        
        # Secret already exists
        self.mock_secret_store.exists.return_value = True
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == []
        assert result.skipped_existing == ["param"]
        assert result.errors == {}
        
        # Should not try to store
        self.mock_secret_store.set.assert_not_called()

    def test_bootstrap_to_store_verification_failure(self):
        """Test bootstrap_to_store handles verification failure."""
        mock_context = Mock()
        mock_param = Mock()
        mock_param.is_secure = True
        mock_param.value = "secret_value"
        
        mock_context.parameters = {"param": mock_param}
        
        self.mock_secret_store.exists.return_value = False
        self.mock_secret_store.set.return_value = None
        self.mock_secret_store.get.return_value = "different_value"  # Verification fails
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == []
        assert result.skipped_existing == []
        assert "param" in result.errors
        assert "verification failed" in result.errors["param"]

    def test_bootstrap_to_store_storage_error(self):
        """Test bootstrap_to_store handles storage errors."""
        mock_context = Mock()
        mock_param = Mock()
        mock_param.is_secure = True
        mock_param.value = "secret_value"
        
        mock_context.parameters = {"param": mock_param}
        
        self.mock_secret_store.exists.return_value = False
        self.mock_secret_store.set.side_effect = Exception("Storage error")
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == []
        assert result.skipped_existing == []
        assert "param" in result.errors
        assert "Storage error" in result.errors["param"]

    def test_delete_from_store_context(self):
        """Test delete_from_store with context."""
        mock_context = Mock()
        mock_param1 = Mock()
        mock_param1.is_secure = True
        
        mock_param2 = Mock()
        mock_param2.is_secure = False
        
        mock_context.parameters = {
            "param1": mock_param1,
            "param2": mock_param2
        }
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        adapter.delete_from_store()
        
        # Should only delete secure parameters
        self.mock_secret_store.delete.assert_called_once_with(
            f"{self.provider_id}/param1"
        )

    def test_delete_from_store_credentials(self):
        """Test delete_from_store with credentials."""
        mock_credentials = Mock()
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            credentials=mock_credentials
        )
        
        adapter.delete_from_store()
        
        # Should delete password
        self.mock_secret_store.delete.assert_called_once_with(
            f"{self.provider_id}/password"
        )

    def test_delete_from_store_handles_errors(self):
        """Test delete_from_store handles errors gracefully."""
        mock_context = Mock()
        mock_param = Mock()
        mock_param.is_secure = True
        
        mock_context.parameters = {"param": mock_param}
        
        # Simulate error during deletion
        self.mock_secret_store.delete.side_effect = Exception("Delete error")
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        # Should not raise error
        adapter.delete_from_store()

    def test_get_parameter_context_secure(self):
        """Test get_parameter with secure context parameter."""
        mock_context = Mock()
        mock_param = Mock()
        mock_param.is_secure = True
        
        mock_context.parameters = {"param": mock_param}
        
        self.mock_secret_store.get.return_value = "secret_value"
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        result = adapter.get_parameter("param")
        
        assert result == "secret_value"
        self.mock_secret_store.get.assert_called_once_with(
            f"{self.provider_id}/param"
        )

    def test_get_parameter_context_plain(self):
        """Test get_parameter with plain context parameter."""
        mock_context = Mock()
        mock_param = Mock()
        mock_param.is_secure = False
        mock_param.value = "plain_value"
        
        mock_context.parameters = {"param": mock_param}
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        result = adapter.get_parameter("param")
        
        assert result == "plain_value"
        self.mock_secret_store.get.assert_not_called()

    def test_get_parameter_credentials_password(self):
        """Test get_parameter with credentials password."""
        mock_credentials = Mock()
        mock_credentials.get_parameter.return_value = "other_value"
        
        self.mock_secret_store.get.return_value = "secret_password"
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            credentials=mock_credentials
        )
        
        result = adapter.get_parameter("password")
        
        assert result == "secret_password"
        self.mock_secret_store.get.assert_called_once_with(
            f"{self.provider_id}/password"
        )

    def test_get_parameter_credentials_other(self):
        """Test get_parameter with credentials other than password."""
        mock_credentials = Mock()
        mock_credentials.get_parameter.return_value = "other_value"
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            credentials=mock_credentials
        )
        
        result = adapter.get_parameter("other_param")
        
        assert result == "other_value"
        mock_credentials.get_parameter.assert_called_once_with("other_param")

    def test_get_parameter_missing_key(self):
        """Test get_parameter with missing key raises error."""
        mock_context = Mock()
        mock_context.parameters = {}
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        with pytest.raises(KeyError):
            adapter.get_parameter("missing_key")

    def test_get_parameter_not_initialized(self):
        """Test get_parameter when adapter is not properly initialized."""
        # We can't create an adapter without context or credentials due to validation
        # So we'll test the error case differently
        with pytest.raises(ValueError, match="Provide either `context` or `credentials`"):
            SecureContextAdapter(
                provider_id=self.provider_id,
                secret_store=self.mock_secret_store,
                context=None,
                credentials=None
            )

    def test_bootstrap_to_store_empty_context(self):
        """Test bootstrap_to_store with empty context."""
        mock_context = Mock()
        mock_context.parameters = {}
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == []
        assert result.skipped_existing == []
        assert result.errors == {}

    def test_bootstrap_to_store_empty_credentials(self):
        """Test bootstrap_to_store with empty credentials."""
        mock_credentials = Mock()
        mock_credentials.decrypted_password = None
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            credentials=mock_credentials
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == []
        assert result.skipped_existing == []
        assert result.errors == {}

    def test_bootstrap_to_store_mixed_context_and_credentials(self):
        """Test bootstrap_to_store with both context and credentials."""
        mock_context = Mock()
        mock_param = Mock()
        mock_param.is_secure = True
        mock_param.value = "context_secret"
        mock_context.parameters = {"param": mock_param}
        
        mock_credentials = Mock()
        mock_credentials.decrypted_password = "credential_password"
        
        self.mock_secret_store.exists.return_value = False
        self.mock_secret_store.get.side_effect = ["context_secret", "credential_password"]
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context,
            credentials=mock_credentials
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == ["param", "password"]
        assert result.skipped_existing == []
        assert result.errors == {}
        
        # Verify both were stored
        assert self.mock_secret_store.set.call_count == 2
        self.mock_secret_store.set.assert_any_call(
            f"{self.provider_id}/param", "context_secret"
        )
        self.mock_secret_store.set.assert_any_call(
            f"{self.provider_id}/password", "credential_password"
        )

    def test_error_handling_in_bootstrap(self):
        """Test comprehensive error handling in bootstrap."""
        mock_context = Mock()
        mock_param1 = Mock()
        mock_param1.is_secure = True
        mock_param1.value = "secret1"
        
        mock_param2 = Mock()
        mock_param2.is_secure = True
        mock_param2.value = "secret2"
        
        mock_context.parameters = {
            "param1": mock_param1,
            "param2": mock_param2
        }
        
        # First secret succeeds, second fails
        self.mock_secret_store.exists.side_effect = [False, False]
        self.mock_secret_store.set.side_effect = [None, Exception("Storage failed")]
        self.mock_secret_store.get.side_effect = ["secret1", "secret1"]
        
        adapter = SecureContextAdapter(
            provider_id=self.provider_id,
            secret_store=self.mock_secret_store,
            context=mock_context
        )
        
        result = adapter.bootstrap_to_store()
        
        assert result.stored == ["param1"]
        assert result.skipped_existing == []
        assert "param2" in result.errors
        assert "Storage failed" in result.errors["param2"]
        
        # First param should be blanked, second should not
        assert mock_param1.value == ""
        assert mock_param2.value == "secret2"
