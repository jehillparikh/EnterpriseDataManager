import pytest
import json
import sys
import os
from functools import wraps
from datetime import date, datetime, timedelta

# Add parent directory to path to allow for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app
from models import db, Fund, FundScheme, FundSchemeDetail, FundFactSheet, Returns, FundHolding, UserInfo
from services import FundService
from api import setup_routes

@pytest.fixture
def client():
    """Configure test Flask client"""
    app.config['TESTING'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    
    with app.app_context():
        db.create_all()
        yield app.test_client()
        db.session.remove()
        db.drop_all()

@pytest.fixture
def sample_fund():
    """Create a sample fund for testing"""
    fund = FundService.create_fund(
        name="Test Mutual Fund",
        amc_name="Test AMC",
        amc_short_name="TAMC",
        short_name="TMF",
        fund_code="TMF001",
        rta_code="RTACODE",
        bse_code="BSE001",
        fund_type="Equity",
        fund_category="Large Cap",
        active=True,
        direct=True
    )
    return fund

@pytest.fixture
def sample_scheme(sample_fund):
    """Create a sample fund scheme for testing"""
    scheme = FundService.create_fund_scheme(
        fund_id=sample_fund.id,
        scheme_code="SCH001",
        scheme_name="Test Scheme",
        plan="Growth",
        option="Direct",
        bse_code="BSESCM001"
    )
    return scheme

@pytest.fixture
def test_user():
    """Create a test user and return with auth token"""
    from werkzeug.security import generate_password_hash
    import jwt
    from datetime import datetime, timedelta
    import os
    
    # Create a test user if it doesn't exist
    test_email = "test@example.com"
    test_mobile = "1234567890"
    
    with app.app_context():
        # Check if user exists, create if not
        user = UserInfo.query.filter_by(email=test_email).first()
        if not user:
            user = UserInfo(
                email=test_email,
                mobile_number=test_mobile,
                password_hash=generate_password_hash("TestPassword123")
            )
            db.session.add(user)
            db.session.commit()
        
        # Generate JWT token for test user
        token = jwt.encode({
            'sub': str(user.id),  # Convert ID to string for JWT compliance
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(minutes=30)
        }, os.environ.get('JWT_SECRET_KEY', 'dev_secret_key'))
        
        return {"user": user, "token": token}

def test_create_fund(client, test_user):
    """Test creating a new fund"""
    fund_data = {
        "name": "HDFC Top 100 Fund",
        "amc_name": "HDFC Mutual Fund",
        "amc_short_name": "HDFC MF",
        "short_name": "HDFC T100",
        "fund_code": "HDFC001",
        "rta_code": "HDFCRTA",
        "bse_code": "BSE100",
        "fund_type": "Equity",
        "fund_category": "Large Cap",
        "active": True,
        "direct": True
    }
    
    # Use the token in the authorization header
    response = client.post(
        '/api/funds',
        data=json.dumps(fund_data),
        content_type='application/json',
        headers={'Authorization': f'Bearer {test_user["token"]}'}
    )
    
    assert response.status_code == 201
    data = json.loads(response.data)
    assert data['name'] == "HDFC Top 100 Fund"
    assert data['amc_name'] == "HDFC Mutual Fund"
    assert data['fund_type'] == "Equity"
    assert 'id' in data

def test_get_fund(client, sample_fund):
    """Test getting a fund by ID"""
    response = client.get(f'/api/funds/{sample_fund.id}')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['name'] == "Test Mutual Fund"
    assert data['amc_name'] == "Test AMC"
    assert data['id'] == sample_fund.id

def test_get_funds_by_amc(client, sample_fund):
    """Test getting all funds for an AMC"""
    response = client.get(f'/api/amcs/{sample_fund.amc_name}/funds')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == 1
    assert data[0]['name'] == "Test Mutual Fund"
    assert data[0]['id'] == sample_fund.id

def test_create_fund_scheme(client, sample_fund, test_user):
    """Test creating a new fund scheme"""
    scheme_data = {
        "fund_id": sample_fund.id,
        "scheme_code": "SCHEME001",
        "scheme_name": "Test Growth Scheme",
        "plan": "Growth",
        "option": "Direct",
        "bse_code": "BSE001SCH"
    }
    
    response = client.post(
        f'/api/funds/{sample_fund.id}/schemes',
        data=json.dumps(scheme_data),
        content_type='application/json',
        headers={'Authorization': f'Bearer {test_user["token"]}'}
    )
    
    assert response.status_code == 201
    data = json.loads(response.data)
    assert data['scheme_code'] == "SCHEME001"
    assert data['scheme_name'] == "Test Growth Scheme"
    assert data['plan'] == "Growth"
    assert 'id' in data

def test_get_schemes_by_fund(client, sample_fund, sample_scheme):
    """Test getting all schemes for a fund"""
    response = client.get(f'/api/funds/{sample_fund.id}/schemes')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == 1
    assert data[0]['scheme_code'] == sample_scheme.scheme_code
    assert data[0]['id'] == sample_scheme.id

def test_create_fund_factsheet(client, sample_scheme, test_user):
    """Test creating a factsheet for a fund scheme"""
    factsheet_data = {
        "fund_manager": "John Doe",
        "fund_house": "Test Fund House",
        "inception_date": "2020-01-01",
        "expense_ratio": 1.5,
        "benchmark_index": "NIFTY 50",
        "category": "Large Cap",
        "risk_level": "Moderate",
        "aum": 1000000000,
        "exit_load": "1% if redeemed within 1 year",
        "holdings_count": 45
    }
    
    response = client.post(
        f'/api/schemes/{sample_scheme.id}/factsheet',
        data=json.dumps(factsheet_data),
        content_type='application/json',
        headers={'Authorization': f'Bearer {test_user["token"]}'}
    )
    
    assert response.status_code == 201
    data = json.loads(response.data)
    assert data['fund_manager'] == "John Doe"
    assert data['benchmark_index'] == "NIFTY 50"
    assert data['risk_level'] == "Moderate"
    assert 'scheme_id' in data
    assert data['scheme_id'] == sample_scheme.id

def test_get_fund_factsheet(client, sample_scheme):
    """Test getting a factsheet for a fund scheme"""
    # First create a factsheet
    factsheet = FundService.create_fund_factsheet(
        scheme_id=sample_scheme.id,
        fund_manager="Jane Smith",
        fund_house="Test Fund House",
        inception_date=date(2019, 1, 1),
        expense_ratio=1.8,
        benchmark_index="NIFTY 100",
        category="Large Cap",
        risk_level="High",
        aum=2000000000,
        exit_load="2% if redeemed within 1 year",
        holdings_count=50
    )
    
    response = client.get(f'/api/schemes/{sample_scheme.id}/factsheet')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['fund_manager'] == "Jane Smith"
    assert data['benchmark_index'] == "NIFTY 100"
    assert data['risk_level'] == "High"
    assert data['scheme_id'] == sample_scheme.id

def test_update_fund_factsheet(client, sample_scheme, test_user):
    """Test updating a factsheet for a fund scheme"""
    # First create a factsheet
    factsheet = FundService.create_fund_factsheet(
        scheme_id=sample_scheme.id,
        fund_manager="Original Manager",
        fund_house="Original Fund House",
        category="Original Category"
    )
    
    update_data = {
        "fund_manager": "Updated Manager",
        "expense_ratio": 1.2,
        "aum": 3000000000
    }
    
    response = client.put(
        f'/api/schemes/{sample_scheme.id}/factsheet',
        data=json.dumps(update_data),
        content_type='application/json',
        headers={'Authorization': f'Bearer {test_user["token"]}'}
    )
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['fund_manager'] == "Updated Manager"
    assert data['expense_ratio'] == 1.2
    assert data['aum'] == 3000000000
    assert data['fund_house'] == "Original Fund House"  # Unchanged field

def test_delete_fund_factsheet(client, sample_scheme, test_user):
    """Test deleting a factsheet for a fund scheme"""
    # First create a factsheet
    factsheet = FundService.create_fund_factsheet(
        scheme_id=sample_scheme.id,
        fund_manager="Test Manager",
        fund_house="Test Fund House",
        category="Test Category"
    )
    
    response = client.delete(
        f'/api/schemes/{sample_scheme.id}/factsheet',
        headers={'Authorization': f'Bearer {test_user["token"]}'}
    )
    
    assert response.status_code == 204
    
    # Verify factsheet is deleted
    try:
        FundService.get_fund_factsheet(sample_scheme.id)
        assert False, "Factsheet should have been deleted"
    except:
        pass

def test_create_fund_returns(client, sample_scheme, test_user):
    """Test creating returns data for a fund scheme"""
    today = date.today()
    returns_data = {
        "date": today.isoformat(),
        "scheme_code": sample_scheme.scheme_code,
        "return_1m": 2.5,
        "return_3m": 7.8,
        "return_6m": 12.3,
        "return_ytd": 9.7,
        "return_1y": 15.2,
        "return_3y": 42.8,
        "return_5y": 76.4
    }
    
    response = client.post(
        f'/api/schemes/{sample_scheme.id}/returns',
        data=json.dumps(returns_data),
        content_type='application/json',
        headers={'Authorization': f'Bearer {test_user["token"]}'}
    )
    
    assert response.status_code == 201
    data = json.loads(response.data)
    assert data['scheme_code'] == sample_scheme.scheme_code
    assert data['return_1m'] == 2.5
    assert data['return_1y'] == 15.2
    assert 'scheme_id' in data
    assert data['scheme_id'] == sample_scheme.id

def test_get_fund_returns(client, sample_scheme):
    """Test getting returns data for a fund scheme"""
    today = date.today()
    
    # First create returns data
    returns = FundService.create_fund_returns(
        scheme_id=sample_scheme.id,
        date=today,
        scheme_code=sample_scheme.scheme_code,
        return_1m=1.5,
        return_3m=4.8,
        return_6m=9.3,
        return_ytd=7.7,
        return_1y=12.2,
        return_3y=32.8,
        return_5y=66.4
    )
    
    response = client.get(f'/api/schemes/{sample_scheme.id}/returns')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]['scheme_code'] == sample_scheme.scheme_code
    assert data[0]['return_1m'] == 1.5
    assert data[0]['return_1y'] == 12.2
    assert data[0]['scheme_id'] == sample_scheme.id

def test_create_fund_holding(client, sample_scheme, test_user):
    """Test creating a fund holding for a scheme"""
    holding_data = {
        "security_name": "HDFC Bank Ltd",
        "isin": "INE040A01034",
        "sector": "Financial Services",
        "asset_type": "Equity",
        "weightage": 9.7,
        "holding_value": 970000000
    }
    
    response = client.post(
        f'/api/schemes/{sample_scheme.id}/holdings',
        data=json.dumps(holding_data),
        content_type='application/json',
        headers={'Authorization': f'Bearer {test_user["token"]}'}
    )
    
    assert response.status_code == 201
    data = json.loads(response.data)
    assert data['security_name'] == "HDFC Bank Ltd"
    assert data['sector'] == "Financial Services"
    assert data['weightage'] == 9.7
    assert data['scheme_id'] == sample_scheme.id

def test_get_fund_holdings(client, sample_scheme):
    """Test getting all holdings for a fund scheme"""
    # Create a few holdings
    holding1 = FundService.create_fund_holding(
        scheme_id=sample_scheme.id,
        security_name="Reliance Industries",
        isin="INE002A01018",
        sector="Energy",
        asset_type="Equity",
        weightage=8.5,
        holding_value=850000000
    )
    
    holding2 = FundService.create_fund_holding(
        scheme_id=sample_scheme.id,
        security_name="TCS",
        isin="INE467B01029",
        sector="Information Technology",
        asset_type="Equity",
        weightage=7.2,
        holding_value=720000000
    )
    
    response = client.get(f'/api/schemes/{sample_scheme.id}/holdings')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == 2
    
    security_names = [h['security_name'] for h in data]
    assert "Reliance Industries" in security_names
    assert "TCS" in security_names

def test_validation_errors(client, sample_fund, test_user):
    """Test validation errors in fund API"""
    # Invalid fund data
    invalid_fund_data = {
        "name": "",  # Empty name
        "amc_name": "Test AMC",
        "fund_type": "Invalid Type"
    }
    
    response = client.post(
        '/api/funds',
        data=json.dumps(invalid_fund_data),
        content_type='application/json',
        headers={'Authorization': f'Bearer {test_user["token"]}'}
    )
    
    assert response.status_code == 400
    assert 'error' in json.loads(response.data)

def test_resource_not_found(client):
    """Test resource not found errors in fund API"""
    # Request non-existent fund
    response = client.get('/api/funds/9999')
    
    assert response.status_code == 404
    assert 'error' in json.loads(response.data)
    
    # Request non-existent scheme
    response = client.get('/api/schemes/9999/factsheet')
    
    assert response.status_code == 404
    assert 'error' in json.loads(response.data)