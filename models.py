from datetime import datetime
from uuid import uuid4

class User:
    """User model representing an application user"""
    
    def __init__(self, username, email, password_hash=None):
        """
        Initialize a User instance
        
        Args:
            username (str): The user's username
            email (str): The user's email address
            password_hash (str, optional): Hashed password for the user
        """
        self.id = str(uuid4())
        self.username = username
        self.email = email
        self.password_hash = password_hash
        self.created_at = datetime.utcnow()
        self.updated_at = self.created_at
    
    def update(self, username=None, email=None, password_hash=None):
        """
        Update user information
        
        Args:
            username (str, optional): New username
            email (str, optional): New email
            password_hash (str, optional): New password hash
        """
        if username is not None:
            self.username = username
        if email is not None:
            self.email = email
        if password_hash is not None:
            self.password_hash = password_hash
        self.updated_at = datetime.utcnow()
    
    def to_dict(self):
        """Convert User object to dictionary representation"""
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }


class Question:
    """Question model representing a question in the system"""
    
    def __init__(self, title, content, user_id, tags=None):
        """
        Initialize a Question instance
        
        Args:
            title (str): The question title
            content (str): The question content
            user_id (str): ID of the user who created the question
            tags (list, optional): List of tags for the question
        """
        self.id = str(uuid4())
        self.title = title
        self.content = content
        self.user_id = user_id
        self.tags = tags or []
        self.created_at = datetime.utcnow()
        self.updated_at = self.created_at
    
    def update(self, title=None, content=None, tags=None):
        """
        Update question information
        
        Args:
            title (str, optional): New title
            content (str, optional): New content
            tags (list, optional): New tags
        """
        if title is not None:
            self.title = title
        if content is not None:
            self.content = content
        if tags is not None:
            self.tags = tags
        self.updated_at = datetime.utcnow()
    
    def to_dict(self):
        """Convert Question object to dictionary representation"""
        return {
            'id': self.id,
            'title': self.title,
            'content': self.content,
            'user_id': self.user_id,
            'tags': self.tags,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
