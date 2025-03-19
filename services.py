import logging
from werkzeug.security import generate_password_hash, check_password_hash
from models import User, Question

logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Base exception for database operations"""
    pass

class ResourceNotFoundError(DatabaseError):
    """Exception for when a resource is not found"""
    pass

class ValidationError(DatabaseError):
    """Exception for validation errors"""
    pass

class UniqueConstraintError(ValidationError):
    """Exception for unique constraint violations"""
    pass

# In-memory storage
_users = {}  # Dictionary to store users, key: user_id
_users_by_username = {}  # Dictionary to lookup users by username
_users_by_email = {}  # Dictionary to lookup users by email
_questions = {}  # Dictionary to store questions, key: question_id

class UserService:
    """Service for User-related operations"""
    
    @staticmethod
    def create_user(username, email, password):
        """
        Create a new user
        
        Args:
            username (str): Username
            email (str): Email address
            password (str): Plaintext password to be hashed
            
        Returns:
            User: The created user
            
        Raises:
            UniqueConstraintError: If username or email already exists
        """
        # Check if username or email already exists
        if username in _users_by_username:
            raise UniqueConstraintError(f"Username '{username}' already exists")
        if email in _users_by_email:
            raise UniqueConstraintError(f"Email '{email}' already exists")
        
        # Hash the password
        password_hash = generate_password_hash(password)
        
        # Create and store the user
        user = User(username=username, email=email, password_hash=password_hash)
        _users[user.id] = user
        _users_by_username[username] = user.id
        _users_by_email[email] = user.id
        
        logger.info(f"User created: {user.id}")
        return user
    
    @staticmethod
    def get_user(user_id):
        """
        Get a user by ID
        
        Args:
            user_id (str): User ID
            
        Returns:
            User: The requested user
            
        Raises:
            ResourceNotFoundError: If user does not exist
        """
        user = _users.get(user_id)
        if not user:
            raise ResourceNotFoundError(f"User with ID '{user_id}' not found")
        return user
    
    @staticmethod
    def get_all_users():
        """
        Get all users
        
        Returns:
            list: List of all users
        """
        return list(_users.values())
    
    @staticmethod
    def update_user(user_id, username=None, email=None, password=None):
        """
        Update a user
        
        Args:
            user_id (str): User ID
            username (str, optional): New username
            email (str, optional): New email
            password (str, optional): New password
            
        Returns:
            User: The updated user
            
        Raises:
            ResourceNotFoundError: If user does not exist
            UniqueConstraintError: If new username or email already exists
        """
        user = UserService.get_user(user_id)
        
        # Check username uniqueness if changing
        if username and username != user.username:
            if username in _users_by_username:
                raise UniqueConstraintError(f"Username '{username}' already exists")
            # Update username lookup
            del _users_by_username[user.username]
            _users_by_username[username] = user.id
        
        # Check email uniqueness if changing
        if email and email != user.email:
            if email in _users_by_email:
                raise UniqueConstraintError(f"Email '{email}' already exists")
            # Update email lookup
            del _users_by_email[user.email]
            _users_by_email[email] = user.id
        
        # Hash the password if provided
        password_hash = generate_password_hash(password) if password else None
        
        # Update the user
        user.update(
            username=username,
            email=email,
            password_hash=password_hash
        )
        
        logger.info(f"User updated: {user.id}")
        return user
    
    @staticmethod
    def delete_user(user_id):
        """
        Delete a user
        
        Args:
            user_id (str): User ID
            
        Raises:
            ResourceNotFoundError: If user does not exist
        """
        user = UserService.get_user(user_id)
        
        # Remove from lookup dictionaries
        del _users_by_username[user.username]
        del _users_by_email[user.email]
        
        # Remove user
        del _users[user_id]
        
        # Delete all questions by this user
        questions_to_delete = [q_id for q_id, q in _questions.items() if q.user_id == user_id]
        for q_id in questions_to_delete:
            del _questions[q_id]
        
        logger.info(f"User deleted: {user_id}")
    
    @staticmethod
    def authenticate_user(email, password):
        """
        Authenticate a user by email and password
        
        Args:
            email (str): User email
            password (str): Plaintext password
            
        Returns:
            User: The authenticated user
            
        Raises:
            ValidationError: If authentication fails
        """
        user_id = _users_by_email.get(email)
        if not user_id:
            raise ValidationError("Invalid email or password")
        
        user = _users[user_id]
        if not check_password_hash(user.password_hash, password):
            raise ValidationError("Invalid email or password")
        
        return user


class QuestionService:
    """Service for Question-related operations"""
    
    @staticmethod
    def create_question(title, content, user_id, tags=None):
        """
        Create a new question
        
        Args:
            title (str): Question title
            content (str): Question content
            user_id (str): ID of the user creating the question
            tags (list, optional): List of tags
            
        Returns:
            Question: The created question
            
        Raises:
            ResourceNotFoundError: If user does not exist
        """
        # Verify user exists
        if user_id not in _users:
            raise ResourceNotFoundError(f"User with ID '{user_id}' not found")
        
        # Create and store the question
        question = Question(
            title=title,
            content=content,
            user_id=user_id,
            tags=tags
        )
        _questions[question.id] = question
        
        logger.info(f"Question created: {question.id}")
        return question
    
    @staticmethod
    def get_question(question_id):
        """
        Get a question by ID
        
        Args:
            question_id (str): Question ID
            
        Returns:
            Question: The requested question
            
        Raises:
            ResourceNotFoundError: If question does not exist
        """
        question = _questions.get(question_id)
        if not question:
            raise ResourceNotFoundError(f"Question with ID '{question_id}' not found")
        return question
    
    @staticmethod
    def get_all_questions():
        """
        Get all questions
        
        Returns:
            list: List of all questions
        """
        return list(_questions.values())
    
    @staticmethod
    def get_questions_by_user(user_id):
        """
        Get all questions by a specific user
        
        Args:
            user_id (str): User ID
            
        Returns:
            list: List of questions by the user
        """
        return [q for q in _questions.values() if q.user_id == user_id]
    
    @staticmethod
    def update_question(question_id, title=None, content=None, tags=None):
        """
        Update a question
        
        Args:
            question_id (str): Question ID
            title (str, optional): New title
            content (str, optional): New content
            tags (list, optional): New tags
            
        Returns:
            Question: The updated question
            
        Raises:
            ResourceNotFoundError: If question does not exist
        """
        question = QuestionService.get_question(question_id)
        
        # Update the question
        question.update(
            title=title,
            content=content,
            tags=tags
        )
        
        logger.info(f"Question updated: {question.id}")
        return question
    
    @staticmethod
    def delete_question(question_id):
        """
        Delete a question
        
        Args:
            question_id (str): Question ID
            
        Raises:
            ResourceNotFoundError: If question does not exist
        """
        # Verify question exists
        QuestionService.get_question(question_id)
        
        # Delete the question
        del _questions[question_id]
        
        logger.info(f"Question deleted: {question_id}")
