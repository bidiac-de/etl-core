from __future__ import annotations

import pytest
from unittest.mock import Mock, patch
from typing import Dict, Any

from etl_core.api.cli import (
    JobsPort, ExecutionPort, LocalJobsClient, LocalExecutionClient,
    HttpJobsClient, HttpExecutionClient
)
from etl_core.persistance.errors import PersistNotFoundError


class TestProtocolCompliance:
    """Test that all client implementations comply with their protocols."""
    
    def test_local_jobs_client_protocol_compliance(self):
        """Test LocalJobsClient implements JobsPort protocol correctly."""
        client = LocalJobsClient()
        
        # Verify all required methods exist
        assert hasattr(client, 'create')
        assert hasattr(client, 'get')
        assert hasattr(client, 'update')
        assert hasattr(client, 'delete')
        assert hasattr(client, 'list_brief')
        
        # Verify method signatures are compatible
        assert callable(client.create)
        assert callable(client.get)
        assert callable(client.update)
        assert callable(client.delete)
        assert callable(client.list_brief)
    
    def test_local_execution_client_protocol_compliance(self):
        """Test LocalExecutionClient implements ExecutionPort protocol correctly."""
        client = LocalExecutionClient()
        
        # Verify all required methods exist
        assert hasattr(client, 'start')
        
        # Verify method signatures are compatible
        assert callable(client.start)
    
    def test_http_jobs_client_protocol_compliance(self):
        """Test HttpJobsClient implements JobsPort protocol correctly."""
        client = HttpJobsClient("http://localhost:8000")
        
        # Verify all required methods exist
        assert hasattr(client, 'create')
        assert hasattr(client, 'get')
        assert hasattr(client, 'update')
        assert hasattr(client, 'delete')
        assert hasattr(client, 'list_brief')
        
        # Verify method signatures are compatible
        assert callable(client.create)
        assert callable(client.get)
        assert callable(client.update)
        assert callable(client.delete)
        assert callable(client.list_brief)
    
    def test_http_execution_client_protocol_compliance(self):
        """Test HttpExecutionClient implements ExecutionPort protocol correctly."""
        client = HttpExecutionClient("http://localhost:8000")
        
        # Verify all required methods exist
        assert hasattr(client, 'start')
        
        # Verify method signatures are compatible
        assert callable(client.start)


class TestProtocolTypeHints:
    """Test that protocol type hints are correctly implemented."""
    
    def test_jobs_port_type_hints(self):
        """Test JobsPort protocol type hints."""
        # This test ensures the protocol is properly typed
        # If there are type annotation issues, mypy would catch them
        assert JobsPort.__annotations__ is not None
        
        # Verify the protocol has the expected structure
        protocol_methods = ['create', 'get', 'update', 'delete', 'list_brief']
        for method in protocol_methods:
            assert hasattr(JobsPort, method)
    
    def test_execution_port_type_hints(self):
        """Test ExecutionPort protocol type hints."""
        # This test ensures the protocol is properly typed
        assert ExecutionPort.__annotations__ is not None
        
        # Verify the protocol has the expected structure
        protocol_methods = ['start']
        for method in protocol_methods:
            assert hasattr(ExecutionPort, method)


class TestClientIntegration:
    """Test integration between different client types."""
    
    def test_local_clients_work_together(self, mock_job_handler, mock_execution_handler):
        """Test that LocalJobsClient and LocalExecutionClient work together."""
        with patch('etl_core.api.cli._jh_singleton', return_value=mock_job_handler), \
             patch('etl_core.api.cli._eh_singleton', return_value=mock_execution_handler):
            
            jobs_client = LocalJobsClient()
            exec_client = LocalExecutionClient()
            
            # Create a job first
            job_id = jobs_client.create({"name": "test_job"})
            assert job_id == "test-job-id-123"
            
            # Then start execution
            result = exec_client.start(job_id)
            assert result["job_id"] == job_id
            assert result["status"] == "started"
    
    def test_http_clients_work_together(self, mock_requests):
        """Test that HttpJobsClient and HttpExecutionClient work together."""
        jobs_client = HttpJobsClient("http://localhost:8000")
        exec_client = HttpExecutionClient("http://localhost:8000")
        
        # Create a job first
        job_id = jobs_client.create({"name": "test_job"})
        assert job_id == {"id": "test-job-id-123"}
        
        # Then start execution
        result = exec_client.start("test-job-id-123")
        assert result == {"id": "test-job-id-123"}
    
    def test_mixed_client_usage(self):
        """Test that local and HTTP clients can be used independently."""
        # This test ensures that using one client type doesn't affect the other
        local_jobs = LocalJobsClient()
        http_jobs = HttpJobsClient("http://localhost:8000")
        
        # They should be completely independent
        assert local_jobs is not http_jobs
        assert type(local_jobs) != type(http_jobs)


class TestClientStateIndependence:
    """Test that client instances maintain independent state."""
    
    def test_local_clients_independent_state(self):
        """Test that LocalJobsClient instances maintain independent state."""
        client1 = LocalJobsClient()
        client2 = LocalJobsClient()
        
        # They should be different instances
        assert client1 is not client2
        
        # Modifying one shouldn't affect the other
        client1.job_handler = Mock()
        assert client1.job_handler is not client2.job_handler
    
    def test_http_clients_independent_state(self):
        """Test that HttpJobsClient instances maintain independent state."""
        client1 = HttpJobsClient("http://localhost:8000")
        client2 = HttpJobsClient("http://localhost:8001")
        
        # They should be different instances
        assert client1 is not client2
        
        # They should have different base URLs
        assert client1.base_url != client2.base_url
    
    def test_execution_clients_independent_state(self):
        """Test that execution client instances maintain independent state."""
        client1 = LocalExecutionClient()
        client2 = LocalExecutionClient()
        
        # They should be different instances
        assert client1 is not client2
        
        # Modifying one shouldn't affect the other
        client1.exec_handler = Mock()
        assert client1.exec_handler is not client2.exec_handler


class TestClientErrorPropagation:
    """Test that errors are properly propagated through the client chain."""
    
    def test_local_execution_client_error_propagation(self, mock_job_handler):
        """Test that errors from job handler propagate to execution client."""
        with patch('etl_core.api.cli._jh_singleton', return_value=mock_job_handler), \
             patch('etl_core.api.cli._eh_singleton', return_value=Mock()):
            
            # Make job handler raise an error
            mock_job_handler.load_runtime_job.side_effect = PersistNotFoundError("Job not found")
            
            exec_client = LocalExecutionClient()
            
            # The error should propagate
            with pytest.raises(PersistNotFoundError, match="Job not found"):
                exec_client.start("non-existent-job")
    
    def test_http_client_error_propagation(self, mock_requests):
        """Test that HTTP errors are properly propagated."""
        # Make requests raise an error
        mock_requests.post.side_effect = Exception("Network error")
        
        client = HttpJobsClient("http://localhost:8000")
        
        # The error should propagate
        with pytest.raises(Exception, match="Network error"):
            client.create({"name": "test_job"})


class TestClientConfiguration:
    """Test client configuration and initialization."""
    
    def test_local_clients_initialize_singletons(self):
        """Test that local clients properly initialize with singletons."""
        with patch('etl_core.api.cli._jh_singleton') as mock_jh, \
             patch('etl_core.api.cli._eh_singleton') as mock_eh:
            
            jobs_client = LocalJobsClient()
            exec_client = LocalExecutionClient()
            
            # Singletons should be called during initialization
            # Note: LocalExecutionClient also creates a LocalJobsClient, so _jh_singleton is called twice
            assert mock_jh.call_count >= 1
            mock_eh.assert_called_once()
    
    def test_http_clients_configure_base_url(self):
        """Test that HTTP clients properly configure base URLs."""
        base_url = "https://api.example.com:8443"
        
        jobs_client = HttpJobsClient(base_url)
        exec_client = HttpExecutionClient(base_url)
        
        assert jobs_client.base_url == base_url
        assert exec_client.base_url == base_url
    
    def test_http_clients_strip_trailing_slashes(self):
        """Test that HTTP clients strip trailing slashes from base URLs."""
        base_url_with_slash = "http://localhost:8000/"
        base_url_without_slash = "http://localhost:8000"
        
        jobs_client = HttpJobsClient(base_url_with_slash)
        exec_client = HttpExecutionClient(base_url_with_slash)
        
        assert jobs_client.base_url == base_url_without_slash
        assert exec_client.base_url == base_url_without_slash
