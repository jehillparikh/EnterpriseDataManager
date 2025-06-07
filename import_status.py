"""
Import status tracking system for background processes
"""
import json
import os
from datetime import datetime
from threading import Lock

class ImportStatusTracker:
    """Track status of background import processes"""
    
    def __init__(self):
        self.status_file = 'import_status.json'
        self._lock = Lock()
        
    def update_status(self, flow_type, status, message, progress=None, stats=None):
        """Update import status for a flow"""
        with self._lock:
            current_status = self._load_status()
            
            current_status[flow_type] = {
                'status': status,  # 'running', 'completed', 'error'
                'message': message,
                'progress': progress,
                'stats': stats,
                'timestamp': datetime.utcnow().isoformat(),
                'last_updated': datetime.utcnow().isoformat()
            }
            
            self._save_status(current_status)
    
    def get_status(self, flow_type=None):
        """Get current status for all flows or specific flow"""
        current_status = self._load_status()
        
        if flow_type:
            return current_status.get(flow_type, {})
        
        return current_status
    
    def clear_status(self, flow_type=None):
        """Clear status for specific flow or all flows"""
        with self._lock:
            current_status = self._load_status()
            
            if flow_type:
                current_status.pop(flow_type, None)
            else:
                current_status = {}
            
            self._save_status(current_status)
    
    def _load_status(self):
        """Load status from file"""
        if not os.path.exists(self.status_file):
            return {}
        
        try:
            with open(self.status_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    
    def _save_status(self, status):
        """Save status to file"""
        try:
            with open(self.status_file, 'w') as f:
                json.dump(status, f, indent=2)
        except Exception as e:
            print(f"Error saving status: {e}")

# Global status tracker instance
status_tracker = ImportStatusTracker()