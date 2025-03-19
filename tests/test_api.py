import pytest
import json
from app import app, setup_api
from services import UserService, QuestionService

@pytest.fixture
def client():
    """Configure test Flask client"""
    app.config['TESTING'] = True
    setup_api(app)
    with app.test_client() as client:
        yield client

def test_create_user(client):
    """Test creating a new user"""
    # Test data
    user_data = {
        'username': 'testuser',
        'email': 'test@example.com',
        'password': 'password123'
    }
    
    # Make request
    response = client.post(
        '/api/users',
        data=json.dumps(user_data),
        content_type='application/json'
    )
    
    # Check response
    assert response.status_code == 201
    data = json.loads(response.data)
    assert data['username'] == 'testuser'
    assert data['email'] == 'test@example.com'
    assert 'id' in data
    assert 'password' not in data

def test_get_user(client):
    """Test getting a user by ID"""
    # Create a test user
    user = UserService.create_user(
        username='getuser',
        email='get@example.com',
        password='password123'
    )
    
    # Make request
    response = client.get(f'/api/users/{user.id}')
    
    # Check response
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['username'] == 'getuser'
    assert data['email'] == 'get@example.com'
    assert data['id'] == user.id

def test_update_user(client):
    """Test updating a user"""
    # Create a test user
    user = UserService.create_user(
        username='updateuser',
        email='update@example.com',
        password='password123'
    )
    
    # Test data
    update_data = {
        'username': 'updateduser',
        'email': 'updated@example.com'
    }
    
    # Make request
    response = client.put(
        f'/api/users/{user.id}',
        data=json.dumps(update_data),
        content_type='application/json'
    )
    
    # Check response
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['username'] == 'updateduser'
    assert data['email'] == 'updated@example.com'
    assert data['id'] == user.id

def test_delete_user(client):
    """Test deleting a user"""
    # Create a test user
    user = UserService.create_user(
        username='deleteuser',
        email='delete@example.com',
        password='password123'
    )
    
    # Make request
    response = client.delete(f'/api/users/{user.id}')
    
    # Check response
    assert response.status_code == 204
    
    # Verify user is deleted
    try:
        UserService.get_user(user.id)
        assert False, "User should have been deleted"
    except:
        pass

def test_create_question(client):
    """Test creating a new question"""
    # Create a test user
    user = UserService.create_user(
        username='questionuser',
        email='question@example.com',
        password='password123'
    )
    
    # Test data
    question_data = {
        'title': 'Test Question',
        'content': 'This is a test question content',
        'user_id': user.id,
        'tags': ['test', 'api']
    }
    
    # Make request
    response = client.post(
        '/api/questions',
        data=json.dumps(question_data),
        content_type='application/json'
    )
    
    # Check response
    assert response.status_code == 201
    data = json.loads(response.data)
    assert data['title'] == 'Test Question'
    assert data['content'] == 'This is a test question content'
    assert data['user_id'] == user.id
    assert data['tags'] == ['test', 'api']
    assert 'id' in data

def test_get_question(client):
    """Test getting a question by ID"""
    # Create a test user
    user = UserService.create_user(
        username='getquser',
        email='getq@example.com',
        password='password123'
    )
    
    # Create a test question
    question = QuestionService.create_question(
        title='Get Question Test',
        content='This is a question to be retrieved',
        user_id=user.id,
        tags=['get', 'test']
    )
    
    # Make request
    response = client.get(f'/api/questions/{question.id}')
    
    # Check response
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['title'] == 'Get Question Test'
    assert data['content'] == 'This is a question to be retrieved'
    assert data['user_id'] == user.id
    assert data['tags'] == ['get', 'test']
    assert data['id'] == question.id

def test_update_question(client):
    """Test updating a question"""
    # Create a test user
    user = UserService.create_user(
        username='updatequser',
        email='updateq@example.com',
        password='password123'
    )
    
    # Create a test question
    question = QuestionService.create_question(
        title='Update Question Test',
        content='This is a question to be updated',
        user_id=user.id,
        tags=['update', 'test']
    )
    
    # Test data
    update_data = {
        'title': 'Updated Question',
        'content': 'This question has been updated',
        'tags': ['updated', 'test']
    }
    
    # Make request
    response = client.put(
        f'/api/questions/{question.id}',
        data=json.dumps(update_data),
        content_type='application/json'
    )
    
    # Check response
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['title'] == 'Updated Question'
    assert data['content'] == 'This question has been updated'
    assert data['tags'] == ['updated', 'test']
    assert data['user_id'] == user.id
    assert data['id'] == question.id

def test_delete_question(client):
    """Test deleting a question"""
    # Create a test user
    user = UserService.create_user(
        username='deletequser',
        email='deleteq@example.com',
        password='password123'
    )
    
    # Create a test question
    question = QuestionService.create_question(
        title='Delete Question Test',
        content='This is a question to be deleted',
        user_id=user.id
    )
    
    # Make request
    response = client.delete(f'/api/questions/{question.id}')
    
    # Check response
    assert response.status_code == 204
    
    # Verify question is deleted
    try:
        QuestionService.get_question(question.id)
        assert False, "Question should have been deleted"
    except:
        pass

def test_get_user_questions(client):
    """Test getting all questions by a user"""
    # Create a test user
    user = UserService.create_user(
        username='userqsuser',
        email='userqs@example.com',
        password='password123'
    )
    
    # Create a few test questions
    question1 = QuestionService.create_question(
        title='User Question 1',
        content='First question by user',
        user_id=user.id,
        tags=['user', 'first']
    )
    
    question2 = QuestionService.create_question(
        title='User Question 2',
        content='Second question by user',
        user_id=user.id,
        tags=['user', 'second']
    )
    
    # Make request
    response = client.get(f'/api/users/{user.id}/questions')
    
    # Check response
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == 2
    
    # Check that both questions are returned
    question_ids = [q['id'] for q in data]
    assert question1.id in question_ids
    assert question2.id in question_ids

def test_validation_errors(client):
    """Test validation errors"""
    # Test data with missing fields
    bad_user_data = {
        'username': 'te',  # Too short
        'email': 'not-an-email'  # Invalid email
    }
    
    # Make request
    response = client.post(
        '/api/users',
        data=json.dumps(bad_user_data),
        content_type='application/json'
    )
    
    # Check response
    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'error' in data

def test_resource_not_found(client):
    """Test resource not found errors"""
    # Make request for non-existent user
    response = client.get('/api/users/nonexistent-id')
    
    # Check response
    assert response.status_code == 404
    data = json.loads(response.data)
    assert 'error' in data
