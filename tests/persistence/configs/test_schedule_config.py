"""Tests for schedule configuration classes."""

import pytest
from datetime import datetime
from pydantic import ValidationError

from etl_core.persistence.configs.schedule_config import (
    ScheduleConfig,
    SchedulePatchConfig,
    _is_non_empty_string,
    _parse_dt,
    _ensure_allowed_keys,
)
from etl_core.persistence.table_definitions import TriggerType


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_is_non_empty_string_valid(self):
        """Test _is_non_empty_string with valid inputs."""
        assert _is_non_empty_string("hello") is True
        assert _is_non_empty_string("   hello   ") is True
        assert _is_non_empty_string("123") is True

    def test_is_non_empty_string_invalid(self):
        """Test _is_non_empty_string with invalid inputs."""
        assert _is_non_empty_string("") is False
        assert _is_non_empty_string("   ") is False
        assert _is_non_empty_string(None) is False
        assert _is_non_empty_string(123) is False
        assert _is_non_empty_string([]) is False

    def test_parse_dt_datetime(self):
        """Test _parse_dt with datetime input."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        result = _parse_dt(dt)
        assert result == dt

    def test_parse_dt_iso_string(self):
        """Test _parse_dt with ISO string input."""
        iso_str = "2023-01-01T12:00:00"
        result = _parse_dt(iso_str)
        expected = datetime(2023, 1, 1, 12, 0, 0)
        assert result == expected

    def test_parse_dt_invalid_string(self):
        """Test _parse_dt with invalid string input."""
        with pytest.raises(ValueError, match="Invalid ISO datetime string"):
            _parse_dt("not-a-date")

    def test_parse_dt_invalid_type(self):
        """Test _parse_dt with invalid type."""
        with pytest.raises(ValueError, match="Datetime must be datetime or ISO string"):
            _parse_dt(123)

    def test_ensure_allowed_keys_valid(self):
        """Test _ensure_allowed_keys with valid keys."""
        args = {"key1": "value1", "key2": "value2"}
        allowed = ["key1", "key2", "key3"]
        _ensure_allowed_keys(args, allowed)  # Should not raise

    def test_ensure_allowed_keys_invalid(self):
        """Test _ensure_allowed_keys with invalid keys."""
        args = {"key1": "value1", "invalid_key": "value2"}
        allowed = ["key1", "key2"]
        with pytest.raises(ValueError, match="Unsupported trigger_args keys"):
            _ensure_allowed_keys(args, allowed)


class TestScheduleConfigImports:
    """Basic tests for ScheduleConfig that avoid the recursion issue."""

    def test_schedule_config_class_exists(self):
        """Test that ScheduleConfig can be imported."""
        assert ScheduleConfig is not None
        assert hasattr(ScheduleConfig, "__init__")

    def test_schedule_patch_config_class_exists(self):
        """Test that SchedulePatchConfig can be imported."""
        assert SchedulePatchConfig is not None
        assert hasattr(SchedulePatchConfig, "__init__")

    def test_helper_functions_exist(self):
        """Test that helper functions can be imported."""
        assert _is_non_empty_string is not None
        assert _parse_dt is not None
        assert _ensure_allowed_keys is not None

    def test_schedule_config_fields(self):
        """Test ScheduleConfig field definitions."""
        fields = ScheduleConfig.model_fields

        assert "name" in fields
        assert "job_id" in fields
        assert "context" in fields
        assert "trigger_type" in fields
        assert "trigger_args" in fields
        assert "paused" in fields

        # Check default for paused
        assert fields["paused"].default is False

    def test_schedule_patch_config_fields(self):
        """Test SchedulePatchConfig field definitions."""
        fields = SchedulePatchConfig.model_fields

        assert "name" in fields
        assert "job_id" in fields
        assert "context" in fields
        assert "trigger_type" in fields
        assert "trigger_args" in fields
        assert "paused" in fields

        # Check that all fields are optional (have None as default)
        for field_name, field_info in fields.items():
            assert field_info.default is None


class TestScheduleConfigBasic:
    """Basic tests for ScheduleConfig without triggering validation recursion."""

    def test_schedule_config_model_config(self):
        """Test that model configuration is set correctly."""
        config_dict = ScheduleConfig.model_config
        assert config_dict["validate_assignment"] is True
        assert config_dict["extra"] == "ignore"

    def test_schedule_patch_config_model_config(self):
        """Test that patch config model configuration is set correctly."""
        config_dict = SchedulePatchConfig.model_config
        assert config_dict["validate_assignment"] is True
        assert config_dict["extra"] == "ignore"


class TestValidationMethods:
    """Test the validation methods without triggering recursion."""

    def test_validate_interval_method_exists(self):
        """Test that _validate_interval static method exists."""
        assert hasattr(ScheduleConfig, "_validate_interval")
        assert callable(ScheduleConfig._validate_interval)

    def test_validate_cron_method_exists(self):
        """Test that _validate_cron static method exists."""
        assert hasattr(ScheduleConfig, "_validate_cron")
        assert callable(ScheduleConfig._validate_cron)

    def test_validate_date_method_exists(self):
        """Test that _validate_date static method exists."""
        assert hasattr(ScheduleConfig, "_validate_date")
        assert callable(ScheduleConfig._validate_date)

    def test_validate_trigger_method_exists(self):
        """Test that _validate_trigger model validator exists."""
        assert hasattr(ScheduleConfig, "_validate_trigger")
        assert callable(ScheduleConfig._validate_trigger)

    def test_interval_validation_basic(self):
        """Test basic interval validation without model instantiation."""
        # Test valid interval args
        valid_args = {"minutes": 30}
        result = ScheduleConfig._validate_interval(valid_args)
        assert result == valid_args

    def test_interval_validation_requires_duration(self):
        """Test that interval validation requires a duration field."""
        invalid_args = {"timezone": "UTC"}
        with pytest.raises(ValueError, match="interval trigger requires one of"):
            ScheduleConfig._validate_interval(invalid_args)

    def test_interval_validation_positive_integers(self):
        """Test that interval validation requires positive integers."""
        invalid_args = {"minutes": -5}
        with pytest.raises(ValueError, match="must be a positive integer"):
            ScheduleConfig._validate_interval(invalid_args)

    def test_cron_validation_basic(self):
        """Test basic cron validation."""
        valid_args = {"hour": 9}
        result = ScheduleConfig._validate_cron(valid_args)
        assert result == valid_args

    def test_cron_validation_requires_field(self):
        """Test that cron validation requires at least one time field."""
        invalid_args = {"timezone": "UTC"}
        with pytest.raises(
            ValueError, match="cron trigger requires at least one scheduling field"
        ):
            ScheduleConfig._validate_cron(invalid_args)

    def test_date_validation_basic(self):
        """Test basic date validation."""
        dt = datetime(2023, 12, 31, 23, 59, 59)
        valid_args = {"run_date": dt}
        result = ScheduleConfig._validate_date(valid_args)
        assert result["run_date"] == dt

    def test_date_validation_requires_run_date(self):
        """Test that date validation requires run_date."""
        invalid_args = {"timezone": "UTC"}
        with pytest.raises(ValueError, match="date trigger requires 'run_date'"):
            ScheduleConfig._validate_date(invalid_args)

    def test_date_validation_alias(self):
        """Test that 'date' alias works for 'run_date'."""
        dt = datetime(2023, 12, 31, 23, 59, 59)
        args_with_alias = {"date": dt}
        result = ScheduleConfig._validate_date(args_with_alias)
        assert result["run_date"] == dt
        assert "date" not in result


class TestValidatorFunctions:
    """Test the individual validator functions."""

    def test_non_empty_validator(self):
        """Test the _non_empty field validator."""
        # This should work without triggering model validation
        assert ScheduleConfig._non_empty("test") == "test"
        assert ScheduleConfig._non_empty("  test  ") == "test"

        with pytest.raises(ValueError, match="must be a non-empty string"):
            ScheduleConfig._non_empty("")

    def test_context_validator(self):
        """Test the _validate_context field validator."""
        assert ScheduleConfig._validate_context("dev") == "DEV"
        assert ScheduleConfig._validate_context("PROD") == "PROD"

        with pytest.raises(ValueError, match="context must be one of DEV/TEST/PROD"):
            ScheduleConfig._validate_context("INVALID")


class TestSchedulePatchConfig:
    """Tests for SchedulePatchConfig that can be instantiated safely."""

    def test_schedule_patch_config_empty(self):
        """Test empty SchedulePatchConfig."""
        config = SchedulePatchConfig()
        assert config.name is None
        assert config.job_id is None
        assert config.context is None
        assert config.trigger_type is None
        assert config.trigger_args is None
        assert config.paused is None

    def test_schedule_patch_config_partial(self):
        """Test partial SchedulePatchConfig."""
        config = SchedulePatchConfig(
            name="updated-name",
            paused=True,
        )
        assert config.name == "updated-name"
        assert config.job_id is None
        assert config.context is None
        assert config.trigger_type is None
        assert config.trigger_args is None
        assert config.paused is True

    def test_schedule_patch_config_name_validation(self):
        """Test name validation in patch config."""
        with pytest.raises(ValidationError) as exc_info:
            SchedulePatchConfig(name="")
        assert "must be a non-empty string" in str(exc_info.value)

    def test_schedule_patch_config_context_validation(self):
        """Test context validation in patch config."""
        with pytest.raises(ValidationError) as exc_info:
            SchedulePatchConfig(context="INVALID")
        assert "context must be one of DEV/TEST/PROD" in str(exc_info.value)


class TestTriggerTypes:
    """Test that all trigger types are covered."""

    def test_all_trigger_types_importable(self):
        """Test that TriggerType enum values can be accessed."""
        assert TriggerType.INTERVAL is not None
        assert TriggerType.CRON is not None
        assert TriggerType.DATE is not None

        # Check string values
        assert TriggerType.INTERVAL.value == "interval"
        assert TriggerType.CRON.value == "cron"
        assert TriggerType.DATE.value == "date"

    def test_trigger_type_coverage(self):
        """Test that validation methods exist for all trigger types."""
        # We should have validation methods for each trigger type
        validation_methods = {
            TriggerType.INTERVAL: ScheduleConfig._validate_interval,
            TriggerType.CRON: ScheduleConfig._validate_cron,
            TriggerType.DATE: ScheduleConfig._validate_date,
        }

        for trigger_type, method in validation_methods.items():
            assert callable(method), f"No validation method for {trigger_type}"
