# 32. src/ingestion/buffer_manager.py
"""Buffer management for data ingestion."""

from collections import deque
from typing import Dict, List, Any, Optional
from datetime import datetime
import threading

from loguru import logger


class BufferManager:
    """Manages buffering of incoming data."""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.buffer = deque(maxlen=max_size)
        self.retry_buffer = deque(maxlen=1000)
        self.lock = threading.Lock()
        self.stats = {
            'added': 0,
            'flushed': 0,
            'dropped': 0,
            'retried': 0
        }
    
    def add(self, data: Dict, retry: bool = False) -> bool:
        """Add data to buffer."""
        with self.lock:
            try:
                if retry:
                    self.retry_buffer.append({
                        'data': data,
                        'timestamp': datetime.now(),
                        'attempts': 1
                    })
                    self.stats['retried'] += 1
                else:
                    self.buffer.append(data)
                    self.stats['added'] += 1
                return True
            except:
                self.stats['dropped'] += 1
                return False
    
    def get_batch(self, size: Optional[int] = None) -> List[Dict]:
        """Get batch of data from buffer."""
        with self.lock:
            if size is None:
                size = len(self.buffer)
            
            batch = []
            for _ in range(min(size, len(self.buffer))):
                batch.append(self.buffer.popleft())
            
            self.stats['flushed'] += len(batch)
            return batch
    
    def should_flush(self) -> bool:
        """Check if buffer should be flushed."""
        return len(self.buffer) >= self.max_size * 0.8
    
    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return len(self.buffer) == 0
    
    def get_stats(self) -> Dict:
        """Get buffer statistics."""
        with self.lock:
            return {
                **self.stats,
                'current_size': len(self.buffer),
                'retry_size': len(self.retry_buffer)
            }