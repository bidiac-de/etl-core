import shutil
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, Mock

from src.etl_core.utils.file_utils import resolve_path, file_exists, ensure_directory


class TestResolvePath:
    """Test cases for resolve_path function."""

    def test_resolve_path_string(self):
        """Test resolve_path with string input."""
        result = resolve_path("test_file.txt")
        assert isinstance(result, Path)
        assert result.name == "test_file.txt"

    def test_resolve_path_path_object(self):
        """Test resolve_path with Path object input."""
        input_path = Path("test_file.txt")
        result = resolve_path(input_path)
        assert isinstance(result, Path)
        assert result == input_path.resolve()

    def test_resolve_path_empty_string(self):
        """Test resolve_path with empty string raises ValueError."""
        with pytest.raises(ValueError, match="A valid filepath must be provided"):
            resolve_path("")

    def test_resolve_path_none(self):
        """Test resolve_path with None raises ValueError."""
        with pytest.raises(ValueError, match="A valid filepath must be provided"):
            resolve_path(None)

    def test_resolve_path_whitespace_only(self):
        """Test resolve_path with whitespace-only string."""
        # Whitespace-only strings are not empty, so they don't raise ValueError
        result = resolve_path("   ")
        assert isinstance(result, Path)

    def test_resolve_path_expands_user(self):
        """Test that resolve_path expands user directory."""
        with patch("pathlib.Path.expanduser") as mock_expanduser:
            mock_path = Mock(spec=Path)
            mock_expanduser.return_value = mock_path
            mock_path.resolve.return_value = mock_path

            resolve_path("~/test_file.txt")
            mock_expanduser.assert_called_once()

    def test_resolve_path_resolves_path(self):
        """Test that resolve_path calls resolve() on the path."""
        with patch("pathlib.Path.resolve") as mock_resolve:
            mock_path = Mock(spec=Path)
            mock_path.expanduser.return_value = mock_path
            mock_resolve.return_value = mock_path

            resolve_path("test_file.txt")
            mock_resolve.assert_called_once()

    def test_resolve_path_relative_path(self):
        """Test resolve_path with relative path."""
        result = resolve_path("./test_file.txt")
        assert isinstance(result, Path)
        assert result.name == "test_file.txt"

    def test_resolve_path_absolute_path(self):
        """Test resolve_path with absolute path."""
        result = resolve_path("/absolute/path/test_file.txt")
        assert isinstance(result, Path)
        assert result.is_absolute()

    def test_resolve_path_with_spaces(self):
        """Test resolve_path with path containing spaces."""
        result = resolve_path("path with spaces/file.txt")
        assert isinstance(result, Path)
        assert "path with spaces" in str(result)

    def test_resolve_path_with_special_characters(self):
        """Test resolve_path with special characters."""
        result = resolve_path("path@with#special$chars/file.txt")
        assert isinstance(result, Path)
        assert "@" in str(result)
        assert "#" in str(result)
        assert "$" in str(result)


class TestFileExists:
    """Test cases for file_exists function."""

    def test_file_exists_true(self):
        """Test file_exists returns True for existing file."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            result = file_exists(temp_path)
            assert result is True
        finally:
            os.unlink(temp_path)

    def test_file_exists_false(self):
        """Test file_exists returns False for non-existent file."""
        result = file_exists("nonexistent_file.txt")
        assert result is False

    def test_file_exists_directory(self):
        """Test file_exists with directory path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = file_exists(temp_dir)
            assert result is True

    def test_file_exists_string_input(self):
        """Test file_exists with string input."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            result = file_exists(str(temp_path))
            assert result is True
        finally:
            os.unlink(temp_path)

    def test_file_exists_path_object_input(self):
        """Test file_exists with Path object input."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = Path(temp_file.name)

        try:
            result = file_exists(temp_path)
            assert result is True
        finally:
            os.unlink(temp_path)

    def test_file_exists_empty_string(self):
        """Test file_exists with empty string returns False."""
        result = file_exists("")
        assert result is False

    def test_file_exists_none(self):
        """Test file_exists with None returns False."""
        result = file_exists(None)
        assert result is False

    def test_file_exists_whitespace_only(self):
        """Test file_exists with whitespace-only string."""
        # Whitespace-only strings are valid paths, so they might exist
        result = file_exists("   ")
        # The result depends on whether the path actually exists
        assert isinstance(result, bool)

    def test_file_exists_invalid_path(self):
        """Test file_exists with invalid path returns False."""
        # Create a path that would cause an error when resolved
        invalid_path = "/" + "a" * 1000  # Very long path that might cause issues

        result = file_exists(invalid_path)
        assert result is False

    def test_file_exists_symlink(self):
        """Test file_exists with symbolic link."""
        import platform

        # Skip symlink test on Windows if we don't have permission
        if platform.system() == "Windows":
            try:
                # Test if we can create symlinks on Windows
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_path = temp_file.name
                    symlink_path = temp_path + "_link"

                    # Try to create a symlink to test permissions
                    os.symlink(temp_path, symlink_path)
                    os.unlink(symlink_path)  # Clean up test symlink
                    os.unlink(temp_path)  # Clean up test file

                    # If we get here, we have permission - run the full test
                    pass
            except (OSError, PermissionError):
                # No permission for symlinks on Windows - skip this test
                pytest.skip(
                    "Windows: No permission to create symlinks "
                    "(run as Administrator or enable Developer Mode)"
                )
                return
        else:
            # On Linux/macOS, symlinks should work
            pass

        # Run the actual symlink test
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Create a symlink
            symlink_path = temp_path + "_link"
            os.symlink(temp_path, symlink_path)

            try:
                result = file_exists(symlink_path)
                assert result is True
            finally:
                os.unlink(symlink_path)
        finally:
            os.unlink(temp_path)

    def test_file_exists_relative_path(self):
        """Test file_exists with relative path."""
        # Create a file in current directory
        test_file = "test_file_exists.txt"
        with open(test_file, "w") as f:
            f.write("test content")

        try:
            result = file_exists(test_file)
            assert result is True
        finally:
            os.unlink(test_file)

    def test_file_exists_absolute_path(self):
        """Test file_exists with absolute path."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = os.path.abspath(temp_file.name)

        try:
            result = file_exists(temp_path)
            assert result is True
        finally:
            os.unlink(temp_path)


class TestEnsureDirectory:
    """Test cases for ensure_directory function."""

    def test_ensure_directory_new_directory(self):
        """Test ensure_directory creates parent directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = os.path.join(temp_dir, "new_subdir", "nested")

            ensure_directory(new_dir)

            # ensure_directory creates the parent directory, not the file path itself
            parent_dir = os.path.dirname(new_dir)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)

    def test_ensure_directory_existing_directory(self):
        """Test ensure_directory with existing directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Directory already exists
            ensure_directory(temp_dir)

            assert os.path.exists(temp_dir)
            assert os.path.isdir(temp_dir)

    def test_ensure_directory_nested_structure(self):
        """Test ensure_directory creates nested parent directory structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_path = os.path.join(temp_dir, "level1", "level2", "level3")

            ensure_directory(nested_path)

            # ensure_directory creates the parent directory structure
            parent_dir = os.path.dirname(nested_path)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)

    def test_ensure_directory_string_input(self):
        """Test ensure_directory with string input."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = os.path.join(temp_dir, "string_test")

            ensure_directory(str(new_dir))

            # ensure_directory creates the parent directory
            parent_dir = os.path.dirname(new_dir)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)

    def test_ensure_directory_path_object_input(self):
        """Test ensure_directory with Path object input."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = Path(temp_dir) / "path_object_test"

            ensure_directory(new_dir)

            # ensure_directory creates the parent directory
            parent_dir = new_dir.parent
            assert parent_dir.exists()
            assert parent_dir.is_dir()

    def test_ensure_directory_file_path(self):
        """Test ensure_directory with file path creates parent directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "subdir", "test_file.txt")

            ensure_directory(file_path)

            # Parent directory should be created
            parent_dir = os.path.dirname(file_path)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)

            # File itself should not exist
            assert not os.path.exists(file_path)

    def test_ensure_directory_multiple_calls(self):
        """Test multiple calls to ensure_directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = os.path.join(temp_dir, "multi_call_test")

            # First call
            ensure_directory(new_dir)
            parent_dir = os.path.dirname(new_dir)
            assert os.path.exists(parent_dir)

            # Second call (should not fail)
            ensure_directory(new_dir)
            assert os.path.exists(parent_dir)

    def test_ensure_directory_with_spaces(self):
        """Test ensure_directory with path containing spaces."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = os.path.join(temp_dir, "path with spaces", "nested")

            ensure_directory(new_dir)

            # ensure_directory creates the parent directory
            parent_dir = os.path.dirname(new_dir)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)

    def test_ensure_directory_with_special_characters(self):
        """Test ensure_directory with special characters in path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = os.path.join(temp_dir, "path@with#special$chars", "nested")

            ensure_directory(new_dir)

            # ensure_directory creates the parent directory
            parent_dir = os.path.dirname(new_dir)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)

    def test_ensure_directory_permissions(self):
        """Test that ensure_directory creates directories with proper permissions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = os.path.join(temp_dir, "permission_test")

            ensure_directory(new_dir)

            # ensure_directory creates the parent directory
            parent_dir = os.path.dirname(new_dir)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)

            # Check that directory is writable
            test_file = os.path.join(parent_dir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test")

            # Clean up
            os.unlink(test_file)

    def test_ensure_directory_empty_string(self):
        """Test ensure_directory with empty string raises ValueError."""
        with pytest.raises(ValueError, match="A valid filepath must be provided"):
            ensure_directory("")

    def test_ensure_directory_none(self):
        """Test ensure_directory with None raises ValueError."""
        with pytest.raises(ValueError, match="A valid filepath must be provided"):
            ensure_directory(None)

    def test_ensure_directory_whitespace_only(self):
        """Test ensure_directory with whitespace-only string."""
        # Whitespace-only strings are not empty, so they don't raise ValueError
        ensure_directory("   ")
        # Should not raise an error

    def test_ensure_directory_root_path(self):
        """
        Test ensure_directory works cross-platform by
        creating a directory under the system temp dir.
        """
        temp_root = tempfile.gettempdir()
        test_root_dir = os.path.join(temp_root, "etl_core_test_root")
        nested_path = os.path.join(test_root_dir, "nested_dir")

        try:
            ensure_directory(nested_path)

            # ensure_directory should create the parent directory (test_root_dir)
            parent_dir = os.path.dirname(nested_path)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)
        finally:
            if os.path.exists(test_root_dir):
                shutil.rmtree(test_root_dir, ignore_errors=True)

    def test_ensure_directory_unicode_path(self):
        """Test ensure_directory with Unicode characters in path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            unicode_dir = os.path.join(temp_dir, "директория_с_кириллицей", "nested")

            ensure_directory(unicode_dir)

            # ensure_directory creates the parent directory
            parent_dir = os.path.dirname(unicode_dir)
            assert os.path.exists(parent_dir)
            assert os.path.isdir(parent_dir)

    def test_ensure_directory_existing_file(self):
        """Test ensure_directory when parent path is a file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a file
            file_path = os.path.join(temp_dir, "test_file.txt")
            with open(file_path, "w") as f:
                f.write("test content")

            # Try to create a directory with the same name as parent
            nested_path = os.path.join(file_path, "nested_dir")

            ensure_directory(nested_path)

            # Clean up
            os.unlink(file_path)
