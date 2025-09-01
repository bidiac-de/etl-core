from abc import ABC, abstractmethod


class RetryStrategy(ABC):
    @abstractmethod
    def should_retry(self, attempt_index: int) -> bool:
        """
        Determine if a retry should be attempted based on the attempt index.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def next_delay(self, attempt_index: int) -> float:
        """
        Calculate the delay before the next retry attempt.
        """


class ConstantRetryStrategy(RetryStrategy):
    def __init__(self, max_retries: int, delay: float = 0):
        self.max_retries = max_retries
        self.delay = delay

    def should_retry(self, attempt_index: int) -> bool:
        return attempt_index < self.max_retries

    def next_delay(self, attempt_index: int) -> float:
        return self.delay


class ExponentialBackoffStrategy(RetryStrategy):
    def __init__(self, max_retries: int, base_delay: float = 1.0, factor: float = 2.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.factor = factor

    def should_retry(self, attempt_index: int) -> bool:
        return attempt_index < self.max_retries

    def next_delay(self, attempt_index: int) -> float:
        return self.base_delay * (self.factor**attempt_index)
