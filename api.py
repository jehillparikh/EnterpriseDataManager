import logging
from flask import Blueprint, request, jsonify, render_template
from marshmallow import ValidationError as SchemaValidationError
from werkzeug.security import generate_password_hash

from schemas import (
    user_schema, users_schema, user_update_schema,
    question_schema, questions_schema, question_update_schema
)
from services import (
    UserService, QuestionService,
    ResourceNotFoundError, ValidationError, UniqueConstraintError
)

logger = logging.getLogger(__name__)

# Create Blueprint for API routes
api_bp = Blueprint('api', __name__, url_prefix='/api')

# Error handler for API routes
@api_bp.errorhandler(Exception)
def handle_error(error):
    """Global error handler for API routes"""
    logger.error(f"API error: {str(error)}")
    
    if isinstance(error, ResourceNotFoundError):
        return jsonify({"error": str(error)}), 404
    elif isinstance(error, (ValidationError, UniqueConstraintError, SchemaValidationError)):
        return jsonify({"error": str(error)}), 400
    else:
        return jsonify({"error": "Internal server error"}), 500

# User routes
@api_bp.route('/users', methods=['POST'])
def create_user():
    """Create a new user"""
    try:
        data = request.get_json()
        
        # Validate request data
        validated_data = user_schema.load(data)
        
        # Create user
        user = UserService.create_user(
            username=validated_data['username'],
            email=validated_data['email'],
            password=validated_data['password']
        )
        
        # Return the created user
        return jsonify(user_schema.dump(user)), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/users', methods=['GET'])
def get_all_users():
    """Get all users"""
    users = UserService.get_all_users()
    return jsonify(users_schema.dump(users)), 200

@api_bp.route('/users/<user_id>', methods=['GET'])
def get_user(user_id):
    """Get a user by ID"""
    user = UserService.get_user(user_id)
    return jsonify(user_schema.dump(user)), 200

@api_bp.route('/users/<user_id>', methods=['PUT', 'PATCH'])
def update_user(user_id):
    """Update a user"""
    try:
        data = request.get_json()
        
        # Validate request data
        validated_data = user_update_schema.load(data)
        
        # Update user
        user = UserService.update_user(
            user_id=user_id,
            username=validated_data.get('username'),
            email=validated_data.get('email'),
            password=validated_data.get('password')
        )
        
        # Return the updated user
        return jsonify(user_schema.dump(user)), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/users/<user_id>', methods=['DELETE'])
def delete_user(user_id):
    """Delete a user"""
    UserService.delete_user(user_id)
    return '', 204

@api_bp.route('/users/<user_id>/questions', methods=['GET'])
def get_user_questions(user_id):
    """Get all questions by a user"""
    # First check if user exists
    UserService.get_user(user_id)
    
    questions = QuestionService.get_questions_by_user(user_id)
    return jsonify(questions_schema.dump(questions)), 200

# Question routes
@api_bp.route('/questions', methods=['POST'])
def create_question():
    """Create a new question"""
    try:
        data = request.get_json()
        
        # Validate request data
        validated_data = question_schema.load(data)
        
        # Create question
        question = QuestionService.create_question(
            title=validated_data['title'],
            content=validated_data['content'],
            user_id=validated_data['user_id'],
            tags=validated_data.get('tags')
        )
        
        # Return the created question
        return jsonify(question_schema.dump(question)), 201
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/questions', methods=['GET'])
def get_all_questions():
    """Get all questions"""
    questions = QuestionService.get_all_questions()
    return jsonify(questions_schema.dump(questions)), 200

@api_bp.route('/questions/<question_id>', methods=['GET'])
def get_question(question_id):
    """Get a question by ID"""
    question = QuestionService.get_question(question_id)
    return jsonify(question_schema.dump(question)), 200

@api_bp.route('/questions/<question_id>', methods=['PUT', 'PATCH'])
def update_question(question_id):
    """Update a question"""
    try:
        data = request.get_json()
        
        # Validate request data
        validated_data = question_update_schema.load(data)
        
        # Update question
        question = QuestionService.update_question(
            question_id=question_id,
            title=validated_data.get('title'),
            content=validated_data.get('content'),
            tags=validated_data.get('tags')
        )
        
        # Return the updated question
        return jsonify(question_schema.dump(question)), 200
    except SchemaValidationError as e:
        return jsonify({"error": e.messages}), 400

@api_bp.route('/questions/<question_id>', methods=['DELETE'])
def delete_question(question_id):
    """Delete a question"""
    QuestionService.delete_question(question_id)
    return '', 204

# Web routes for documentation
web_bp = Blueprint('web', __name__)

@web_bp.route('/')
def index():
    """Render the homepage"""
    return render_template('index.html')

@web_bp.route('/docs')
def docs():
    """Render the API documentation page"""
    return render_template('documentation.html')

def setup_routes(app):
    """
    Register blueprints with the Flask application
    
    Args:
        app: Flask application
    """
    app.register_blueprint(api_bp)
    app.register_blueprint(web_bp)
    
    # Register error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        if request.path.startswith('/api/'):
            return jsonify({"error": "Resource not found"}), 404
        return render_template('index.html'), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        if request.path.startswith('/api/'):
            return jsonify({"error": "Internal server error"}), 500
        return render_template('index.html'), 500
