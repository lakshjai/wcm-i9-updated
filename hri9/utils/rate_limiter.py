#!/usr/bin/env python3
"""
Rate limiter for API calls to prevent rate limit errors.

This module provides rate limiting functionality to ensure API calls
stay within acceptable limits and reduce the likelihood of rate limit errors.
"""

import time
import threading
from collections import deque
from typing import Optional
from ..utils.logging_config import logger
from ..config import settings


class RateLimiter:
    """
    Thread-safe rate limiter for API calls.
    
    Implements a sliding window rate limiter to ensure API calls
    stay within specified limits.
    """
    
    def __init__(self, max_requests_per_minute: int = None, min_delay_seconds: float = None):
        """
        Initialize the rate limiter.
        
        Args:
            max_requests_per_minute: Maximum requests allowed per minute
            min_delay_seconds: Minimum delay between requests in seconds
        """
        self.max_requests_per_minute = max_requests_per_minute or settings.API_RATE_LIMIT_RPM
        self.min_delay_seconds = min_delay_seconds or settings.API_RATE_LIMIT_DELAY
        
        # Sliding window to track request timestamps
        self._request_times = deque()
        self._lock = threading.RLock()
        self._last_request_time = 0
        
        logger.info(f"Rate limiter initialized: {self.max_requests_per_minute} RPM, {self.min_delay_seconds}s min delay")
    
    def acquire(self) -> float:
        """
        Acquire permission to make an API request.
        
        This method will block if necessary to ensure rate limits are respected.
        
        Returns:
            float: Actual delay time that was applied (for monitoring)
        """
        with self._lock:
            current_time = time.time()
            
            # Clean up old request times (older than 1 minute)
            cutoff_time = current_time - 60.0
            while self._request_times and self._request_times[0] < cutoff_time:
                self._request_times.popleft()
            
            # Calculate delays needed
            delay_for_rate_limit = self._calculate_rate_limit_delay(current_time)
            delay_for_min_interval = self._calculate_min_delay(current_time)
            
            # Use the maximum of both delays
            total_delay = max(delay_for_rate_limit, delay_for_min_interval)
            
            if total_delay > 0:
                logger.debug(f"Rate limiter applying {total_delay:.2f}s delay")
                time.sleep(total_delay)
                current_time = time.time()
            
            # Record this request
            self._request_times.append(current_time)
            self._last_request_time = current_time
            
            return total_delay
    
    def _calculate_rate_limit_delay(self, current_time: float) -> float:
        """
        Calculate delay needed to respect rate limit.
        
        Args:
            current_time: Current timestamp
            
        Returns:
            float: Delay needed in seconds
        """
        if len(self._request_times) < self.max_requests_per_minute:
            return 0.0
        
        # If we're at the rate limit, calculate when we can make the next request
        oldest_request_time = self._request_times[0]
        time_since_oldest = current_time - oldest_request_time
        
        if time_since_oldest < 60.0:
            # Need to wait until the oldest request is more than 1 minute old
            delay_needed = 60.0 - time_since_oldest + 0.1  # Add small buffer
            return delay_needed
        
        return 0.0
    
    def _calculate_min_delay(self, current_time: float) -> float:
        """
        Calculate delay needed to respect minimum interval.
        
        Args:
            current_time: Current timestamp
            
        Returns:
            float: Delay needed in seconds
        """
        if self._last_request_time == 0:
            return 0.0
        
        time_since_last = current_time - self._last_request_time
        if time_since_last < self.min_delay_seconds:
            return self.min_delay_seconds - time_since_last
        
        return 0.0
    
    def get_current_rate(self) -> float:
        """
        Get current request rate (requests per minute).
        
        Returns:
            float: Current rate in requests per minute
        """
        with self._lock:
            current_time = time.time()
            cutoff_time = current_time - 60.0
            
            # Count requests in the last minute
            recent_requests = sum(1 for t in self._request_times if t > cutoff_time)
            return recent_requests
    
    def get_stats(self) -> dict:
        """
        Get rate limiter statistics.
        
        Returns:
            dict: Statistics including current rate, limits, etc.
        """
        with self._lock:
            return {
                "max_requests_per_minute": self.max_requests_per_minute,
                "min_delay_seconds": self.min_delay_seconds,
                "current_rate_rpm": self.get_current_rate(),
                "requests_in_window": len(self._request_times),
                "last_request_time": self._last_request_time
            }


# Global rate limiter instance
_global_rate_limiter: Optional[RateLimiter] = None
_rate_limiter_lock = threading.Lock()


def get_rate_limiter() -> RateLimiter:
    """
    Get the global rate limiter instance.
    
    Returns:
        RateLimiter: Global rate limiter instance
    """
    global _global_rate_limiter
    
    if _global_rate_limiter is None:
        with _rate_limiter_lock:
            if _global_rate_limiter is None:
                _global_rate_limiter = RateLimiter()
    
    return _global_rate_limiter


def reset_rate_limiter():
    """Reset the global rate limiter (useful for testing)."""
    global _global_rate_limiter
    
    with _rate_limiter_lock:
        _global_rate_limiter = None