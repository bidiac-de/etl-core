"""
Tests for verifying thread-safety of the singleton pattern in singletons.py
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List
from unittest.mock import patch

import pytest

import etl_core.singletons as singletons_module
from etl_core.singletons import (
    job_handler,
    execution_records_handler,
    context_handler,
    credentials_handler,
    schedule_handler,
    reset_singletons,
)


@pytest.fixture(autouse=True)
def reset_state():
    """Reset all singletons before and after each test to ensure test isolation"""
    reset_singletons()
    yield
    reset_singletons()


class TestSingletonThreadSafety:
    """Tests for thread-safe singleton access"""

    def test_job_handler_returns_same_instance(self) -> None:
        """Verify job_handler returns the same instance on multiple calls"""
        h1 = job_handler()
        h2 = job_handler()
        assert h1 is h2

    def test_execution_records_handler_returns_same_instance(self) -> None:
        """Verify execution_records_handler returns the same instance"""
        h1 = execution_records_handler()
        h2 = execution_records_handler()
        assert h1 is h2

    def test_context_handler_returns_same_instance(self) -> None:
        """Verify context_handler returns the same instance"""
        h1 = context_handler()
        h2 = context_handler()
        assert h1 is h2

    def test_credentials_handler_returns_same_instance(self) -> None:
        """Verify credentials_handler returns the same instance"""
        h1 = credentials_handler()
        h2 = credentials_handler()
        assert h1 is h2

    def test_schedule_handler_returns_same_instance(self) -> None:
        """Verify schedule_handler returns the same instance"""
        h1 = schedule_handler()
        h2 = schedule_handler()
        assert h1 is h2

    def test_concurrent_job_handler_access_returns_same_instance(self) -> None:
        """
        Verify that concurrent access to job_handler from multiple threads
        returns the same instance (no race condition)
        """
        instances: List[Any] = []
        errors: List[Exception] = []
        num_threads = 20

        def get_handler():
            try:
                instances.append(job_handler())
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=get_handler) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors occurred: {errors}"
        assert len(instances) == num_threads
        # All instances should be the same object
        first = instances[0]
        assert all(inst is first for inst in instances)

    def test_concurrent_context_handler_access_returns_same_instance(self) -> None:
        """
        Verify that concurrent access to context_handler from multiple threads
        returns the same instance
        """
        instances: List[Any] = []
        num_threads = 20

        def get_handler():
            instances.append(context_handler())

        threads = [threading.Thread(target=get_handler) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(instances) == num_threads
        first = instances[0]
        assert all(inst is first for inst in instances)

    def test_concurrent_credentials_handler_access_returns_same_instance(self) -> None:
        """
        Verify that concurrent access to credentials_handler from multiple threads
        returns the same instance
        """
        instances: List[Any] = []
        num_threads = 20

        def get_handler():
            instances.append(credentials_handler())

        threads = [threading.Thread(target=get_handler) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(instances) == num_threads
        first = instances[0]
        assert all(inst is first for inst in instances)


class TestSingletonWithDelayedInit:
    """
    Tests that simulate slow initialization to verify thread-safety
    """

    def test_slow_init_still_returns_single_instance(self) -> None:
        """
        Simulate slow handler initialization and verify only one instance
        is created even with concurrent access
        """
        creation_count = 0
        original_init = singletons_module.ContextHandler.__init__

        def slow_init(self, *args, **kwargs):
            nonlocal creation_count
            creation_count += 1
            time.sleep(0.05)  # Simulate slow initialization
            original_init(self, *args, **kwargs)

        with patch.object(
            singletons_module.ContextHandler, "__init__", slow_init
        ):
            instances: List[Any] = []
            num_threads = 10

            def get_handler():
                instances.append(context_handler())

            threads = [
                threading.Thread(target=get_handler) for _ in range(num_threads)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert len(instances) == num_threads
            # Only one instance should have been created
            assert creation_count == 1
            first = instances[0]
            assert all(inst is first for inst in instances)


class TestResetSingletons:
    """Tests for the reset_singletons function"""

    def test_reset_clears_all_singletons(self) -> None:
        """Verify reset_singletons clears all cached instances"""
        # First, create some singletons
        h1 = job_handler()
        h2 = context_handler()
        h3 = credentials_handler()

        # Reset
        reset_singletons()

        # get new instances, should be different objects
        h1_new = job_handler()
        h2_new = context_handler()
        h3_new = credentials_handler()

        assert h1 is not h1_new
        assert h2 is not h2_new
        assert h3 is not h3_new

    def test_reset_is_thread_safe(self) -> None:
        """Verify reset_singletons is thread-safe"""
        errors: List[Exception] = []

        def reset_and_get():
            try:
                reset_singletons()
                job_handler()
                context_handler()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reset_and_get) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors during concurrent reset: {errors}"


class TestDoubleCheckedLocking:
    """Tests to verify the double-checked locking pattern works correctly"""

    def test_outer_check_prevents_lock_contention(self) -> None:
        """
        Verify that the outer None check prevents unnecessary lock acquisition
        by ensuring fast path access after initialization
        """
        # create the singleton
        first_instance = job_handler()

        # measure time for many accesses (fast, no lock)
        import time
        start = time.perf_counter()
        for _ in range(10000):
            h = job_handler()
            assert h is first_instance
        elapsed = time.perf_counter() - start

        # should complete very quickly (< 1 second for 10000 calls)
        assert elapsed < 1.0, f"Access took too long: {elapsed}s - possible lock contention"

    def test_singleton_identity_preserved_across_threads(self) -> None:
        """
        Verify that the double-checked locking preserves singleton identity.
        """
        # get initial instance
        initial = job_handler()

        instances: List[Any] = []

        def access_handler():
            for _ in range(100):
                instances.append(job_handler())

        threads = [threading.Thread(target=access_handler) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All 1000 accesses should return the same instance
        assert len(instances) == 1000
        assert all(inst is initial for inst in instances)


class TestThreadPoolConcurrency:
    """Tests using ThreadPoolExecutor for more realistic concurrency"""

    def test_threadpool_concurrent_access(self) -> None:
        """
        Test concurrent singleton access using ThreadPoolExecutor
        """
        num_workers = 50
        instances: List[Any] = []

        def get_job_handler(_: int) -> Any:
            return job_handler()

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(get_job_handler, i) for i in range(num_workers)]
            for future in as_completed(futures):
                instances.append(future.result())

        assert len(instances) == num_workers
        first = instances[0]
        assert all(inst is first for inst in instances)

    def test_threadpool_mixed_handlers(self) -> None:
        """
        Test concurrent access to different handlers from ThreadPoolExecutor
        """
        handlers = [
            job_handler,
            context_handler,
            credentials_handler,
            execution_records_handler,
            schedule_handler,
        ]
        num_workers = 100
        results: List[tuple] = []

        def get_handler(idx: int) -> tuple:
            handler_func = handlers[idx % len(handlers)]
            return (handler_func.__name__, handler_func())

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(get_handler, i) for i in range(num_workers)]
            for future in as_completed(futures):
                results.append(future.result())

        # Group by handler name and verify all instances are the same
        from collections import defaultdict
        by_name: dict = defaultdict(list)
        for name, instance in results:
            by_name[name].append(instance)

        for name, instances in by_name.items():
            first = instances[0]
            assert all(
                inst is first for inst in instances
            ), f"Multiple instances created for {name}"
