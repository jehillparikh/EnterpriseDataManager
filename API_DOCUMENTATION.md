# Mutual Fund Management API Documentation

## Overview

This API provides a comprehensive set of endpoints for managing mutual funds, including user management, KYC verification, bank details, AMCs, funds, fund schemes, fund details, portfolio management, and fund holdings.

### Key Features

- **User Authentication**: JWT-based authentication system
- **KYC Verification**: Integration with Hyperverge for KYC verification
- **Fund Management**: Complete CRUD operations for AMCs, funds, and schemes
- **Portfolio Tracking**: User portfolio management with current value calculation
- **Fund Holdings**: Track individual securities within mutual fund schemes
- **Returns Analysis**: Track historical returns across multiple time periods
- **BSE Star Integration**: Client registration and transaction processing

## Base URL

All API requests should be prefixed with `/api`.

## Authentication

Most endpoints require authentication using a JWT token. To authenticate, include the token in the `Authorization` header:

```
Authorization: Bearer <your_token>
```

You can obtain a token by calling the login endpoint.

## Error Handling

The API returns standard HTTP status codes:

- 200: Success
- 201: Created
- 400: Bad Request (validation errors)
- 401: Unauthorized
- 403: Forbidden
- 404: Not Found
- 500: Internal Server Error

Error responses have the following format:

```json
{
  "error": "Error message"
}
```

## Endpoints

### Authentication

#### Register a new user

```
POST /api/auth/register
```

Request body:
```json
{
  "email": "user@example.com",
  "mobile_number": "9876543210",
  "password": "password123"
}
```

Response:
```json
{
  "message": "User registered successfully",
  "user": {
    "id": 1,
    "email": "user@example.com",
    "mobile_number": "9876543210"
  }
}
```

#### Login

```
POST /api/auth/login
```

Request body:
```json
{
  "email": "user@example.com",
  "password": "password123"
}
```

Response:
```json
{
  "message": "Login successful",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user": {
    "id": 1,
    "email": "user@example.com",
    "mobile_number": "9876543210"
  }
}
```

### User Management

#### Get user profile

```
GET /api/users/profile
```

Response:
```json
{
  "id": 1,
  "email": "user@example.com",
  "mobile_number": "9876543210",
  "created_at": "2023-01-01T00:00:00Z",
  "updated_at": "2023-01-01T00:00:00Z"
}
```

#### Update user profile

```
PUT /api/users/profile
```

Request body:
```json
{
  "email": "new.email@example.com",
  "mobile_number": "9876543211",
  "password": "newpassword123"
}
```

Response:
```json
{
  "message": "Profile updated successfully",
  "user": {
    "id": 1,
    "email": "new.email@example.com",
    "mobile_number": "9876543211",
    "updated_at": "2023-01-02T00:00:00Z"
  }
}
```

#### Delete user profile

```
DELETE /api/users/profile
```

Response:
```json
{
  "message": "Account deleted successfully"
}
```

### KYC

#### Create KYC details

```
POST /api/users/kyc
```

Request body:
```json
{
  "pan": "ABCDE1234F",
  "tax_status": "01",
  "occ_code": "02",
  "first_name": "John",
  "middle_name": "",
  "last_name": "Doe",
  "dob": "1990-01-01",
  "gender": "M",
  "address": "123 Main St",
  "city": "Mumbai",
  "state": "MH",
  "pincode": "400001",
  "phone": "9876543210",
  "income_slab": 32
}
```

Response:
```json
{
  "message": "KYC details added successfully",
  "kyc": {
    "id": 1,
    "user_id": 1,
    "pan": "ABCDE1234F",
    "first_name": "John",
    "last_name": "Doe"
  }
}
```

#### Get KYC details

```
GET /api/users/kyc
```

Response:
```json
{
  "id": 1,
  "user_id": 1,
  "pan": "ABCDE1234F",
  "tax_status": "01",
  "occ_code": "02",
  "first_name": "John",
  "middle_name": "",
  "last_name": "Doe",
  "dob": "1990-01-01",
  "gender": "M",
  "address": "123 Main St",
  "city": "Mumbai",
  "state": "MH",
  "pincode": "400001",
  "phone": "9876543210",
  "income_slab": 32
}
```

#### Update KYC details

```
PUT /api/users/kyc
```

Request body:
```json
{
  "address": "456 New St",
  "city": "Delhi",
  "state": "DL",
  "pincode": "110001"
}
```

Response:
```json
{
  "message": "KYC details updated successfully",
  "kyc": {
    "id": 1,
    "user_id": 1,
    "pan": "ABCDE1234F",
    "first_name": "John",
    "last_name": "Doe"
  }
}
```

### Bank Details

#### Get all banks

```
GET /api/banks
```

Response:
```json
[
  {
    "id": 1,
    "name": "State Bank of India"
  },
  {
    "id": 2,
    "name": "HDFC Bank"
  }
]
```

#### Get branches for a bank

```
GET /api/banks/{bank_id}/branches
```

Response:
```json
[
  {
    "id": 1,
    "bank_id": 1,
    "branch_name": "Main Branch",
    "branch_city": "Mumbai",
    "branch_address": "123 SBI Road",
    "ifsc_code": "SBIN0000123",
    "micr_code": "400002001"
  }
]
```

#### Add bank details

```
POST /api/users/bank-details
```

Request body:
```json
{
  "branch_id": 1,
  "account_number": "1234567890",
  "account_type_bse": "SB"
}
```

Response:
```json
{
  "message": "Bank details added successfully",
  "bank_detail": {
    "id": 1,
    "user_id": 1,
    "branch_id": 1,
    "account_number": "1234567890",
    "account_type_bse": "SB"
  }
}
```

#### Get bank details

```
GET /api/users/bank-details
```

Response:
```json
[
  {
    "id": 1,
    "user_id": 1,
    "branch_id": 1,
    "account_number": "1234567890",
    "account_type_bse": "SB"
  }
]
```

### AMC and Fund Management

#### Get all AMCs

```
GET /api/amcs
```

Response:
```json
[
  {
    "id": 1,
    "name": "HDFC Mutual Fund",
    "short_name": "HDFC MF",
    "fund_code": "HDFC",
    "bse_code": "HDFC001",
    "active": true
  }
]
```

#### Create a new AMC

```
POST /api/amcs
```

Request body:
```json
{
  "name": "ICICI Prudential Mutual Fund",
  "short_name": "ICICI MF",
  "fund_code": "ICICI",
  "bse_code": "ICICI001",
  "active": true
}
```

Response:
```json
{
  "message": "AMC created successfully",
  "amc": {
    "id": 2,
    "name": "ICICI Prudential Mutual Fund",
    "short_name": "ICICI MF",
    "fund_code": "ICICI",
    "bse_code": "ICICI001",
    "active": true
  }
}
```

#### Get an AMC by ID

```
GET /api/amcs/{amc_id}
```

Response:
```json
{
  "id": 1,
  "name": "HDFC Mutual Fund",
  "short_name": "HDFC MF",
  "fund_code": "HDFC",
  "bse_code": "HDFC001",
  "active": true
}
```

#### Get funds for an AMC

```
GET /api/amcs/{amc_id}/funds
```

Response:
```json
[
  {
    "id": 1,
    "name": "HDFC Top 100 Fund",
    "short_name": "HDFC Top 100",
    "amc_id": 1,
    "rta_code": "TOP100",
    "bse_code": "HDFC001001",
    "active": true,
    "direct": false
  }
]
```

#### Create a new fund

```
POST /api/amcs/{amc_id}/funds
```

Request body:
```json
{
  "name": "HDFC Mid-Cap Opportunities Fund",
  "short_name": "HDFC Mid-Cap",
  "rta_code": "MIDCAP",
  "bse_code": "HDFC001002",
  "active": true,
  "direct": false
}
```

Response:
```json
{
  "message": "Fund created successfully",
  "fund": {
    "id": 2,
    "name": "HDFC Mid-Cap Opportunities Fund",
    "short_name": "HDFC Mid-Cap",
    "amc_id": 1,
    "rta_code": "MIDCAP",
    "bse_code": "HDFC001002",
    "active": true,
    "direct": false
  }
}
```

#### Get a fund by ID

```
GET /api/funds/{fund_id}
```

Response:
```json
{
  "id": 1,
  "name": "HDFC Top 100 Fund",
  "short_name": "HDFC Top 100",
  "amc_id": 1,
  "rta_code": "TOP100",
  "bse_code": "HDFC001001",
  "active": true,
  "direct": false
}
```

#### Get schemes for a fund

```
GET /api/funds/{fund_id}/schemes
```

Response:
```json
[
  {
    "id": 1,
    "fund_id": 1,
    "scheme_code": "TOP100G",
    "plan": "G",
    "option": null,
    "bse_code": "HDFC001001G"
  }
]
```

#### Create a new fund scheme

```
POST /api/funds/{fund_id}/schemes
```

Request body:
```json
{
  "scheme_code": "TOP100D",
  "plan": "D",
  "option": "Payout",
  "bse_code": "HDFC001001D"
}
```

Response:
```json
{
  "message": "Fund scheme created successfully",
  "scheme": {
    "id": 2,
    "fund_id": 1,
    "scheme_code": "TOP100D",
    "plan": "D",
    "option": "Payout",
    "bse_code": "HDFC001001D"
  }
}
```

#### Get a fund scheme by ID

```
GET /api/schemes/{scheme_id}
```

Response:
```json
{
  "id": 1,
  "fund_id": 1,
  "scheme_code": "TOP100G",
  "plan": "G",
  "option": null,
  "bse_code": "HDFC001001G"
}
```

### Fund Scheme Details

#### Create scheme details

```
POST /api/schemes/{scheme_id}/details
```

Request body:
```json
{
  "nav": 123.45,
  "expense_ratio": 1.2,
  "fund_manager": "John Smith",
  "aum": 10000000000,
  "risk_level": "Moderate",
  "benchmark": "Nifty 50"
}
```

Response:
```json
{
  "message": "Fund scheme details created successfully",
  "details": {
    "id": 1,
    "scheme_id": 1,
    "nav": 123.45,
    "expense_ratio": 1.2,
    "fund_manager": "John Smith",
    "aum": 10000000000,
    "risk_level": "Moderate",
    "benchmark": "Nifty 50"
  }
}
```

### Fund Factsheet

#### Create fund factsheet

```
POST /api/schemes/{scheme_id}/factsheet
```

Request body:
```json
{
  "fund_manager": "John Smith",
  "fund_house": "HDFC Mutual Fund",
  "inception_date": "2000-01-01",
  "expense_ratio": 1.2,
  "benchmark_index": "Nifty 50",
  "category": "Large Cap",
  "risk_level": "Moderate",
  "aum": 10000000000,
  "exit_load": "1% if redeemed within 1 year",
  "holdings_count": 45
}
```

Response:
```json
{
  "message": "Fund factsheet created successfully",
  "factsheet": {
    "id": 1,
    "scheme_id": 1,
    "fund_manager": "John Smith",
    "fund_house": "HDFC Mutual Fund",
    "inception_date": "2000-01-01",
    "expense_ratio": 1.2,
    "benchmark_index": "Nifty 50",
    "category": "Large Cap",
    "risk_level": "Moderate",
    "aum": 10000000000,
    "exit_load": "1% if redeemed within 1 year",
    "holdings_count": 45,
    "last_updated": "2023-01-01T00:00:00Z"
  }
}
```

#### Get fund factsheet

```
GET /api/schemes/{scheme_id}/factsheet
```

Response:
```json
{
  "id": 1,
  "scheme_id": 1,
  "fund_manager": "John Smith",
  "fund_house": "HDFC Mutual Fund",
  "inception_date": "2000-01-01",
  "expense_ratio": 1.2,
  "benchmark_index": "Nifty 50",
  "category": "Large Cap",
  "risk_level": "Moderate",
  "aum": 10000000000,
  "exit_load": "1% if redeemed within 1 year",
  "holdings_count": 45,
  "last_updated": "2023-01-01T00:00:00Z"
}
```

#### Update fund factsheet

```
PUT /api/schemes/{scheme_id}/factsheet
```

Request body:
```json
{
  "fund_manager": "Jane Doe",
  "aum": 12000000000,
  "holdings_count": 50
}
```

Response:
```json
{
  "message": "Fund factsheet updated successfully",
  "factsheet": {
    "id": 1,
    "scheme_id": 1,
    "fund_manager": "Jane Doe",
    "fund_house": "HDFC Mutual Fund",
    "inception_date": "2000-01-01",
    "expense_ratio": 1.2,
    "benchmark_index": "Nifty 50",
    "category": "Large Cap",
    "risk_level": "Moderate",
    "aum": 12000000000,
    "exit_load": "1% if redeemed within 1 year",
    "holdings_count": 50,
    "last_updated": "2023-01-02T00:00:00Z"
  }
}
```

#### Delete fund factsheet

```
DELETE /api/schemes/{scheme_id}/factsheet
```

Response:
```json
{
  "message": "Fund factsheet deleted successfully"
}
```

### Fund Returns

#### Create fund returns

```
POST /api/schemes/{scheme_id}/returns
```

Request body:
```json
{
  "date": "2023-01-01",
  "return_1m": 1.2,
  "return_3m": 3.5,
  "return_6m": 7.8,
  "return_ytd": 8.5,
  "return_1y": 12.5,
  "return_3y": 15.2,
  "return_5y": 18.5,
  "scheme_code": "TOP100G"
}
```

Response:
```json
{
  "message": "Fund returns created successfully",
  "returns": {
    "id": 1,
    "scheme_id": 1,
    "date": "2023-01-01",
    "return_1m": 1.2,
    "return_3m": 3.5,
    "return_6m": 7.8,
    "return_ytd": 8.5,
    "return_1y": 12.5,
    "return_3y": 15.2,
    "return_5y": 18.5,
    "scheme_code": "TOP100G"
  }
}
```

#### Get fund returns

```
GET /api/schemes/{scheme_id}/returns
```

Response:
```json
[
  {
    "id": 1,
    "scheme_id": 1,
    "date": "2023-01-01",
    "return_1m": 1.2,
    "return_3m": 3.5,
    "return_6m": 7.8,
    "return_ytd": 8.5,
    "return_1y": 12.5,
    "return_3y": 15.2,
    "return_5y": 18.5,
    "scheme_code": "TOP100G"
  }
]
```

#### Update fund returns

```
PUT /api/schemes/{scheme_id}/returns/{date}
```

Request body:
```json
{
  "return_1m": 1.5,
  "return_3m": 4.0
}
```

Response:
```json
{
  "message": "Fund returns updated successfully",
  "returns": {
    "id": 1,
    "scheme_id": 1,
    "date": "2023-01-01",
    "return_1m": 1.5,
    "return_3m": 4.0,
    "return_6m": 7.8,
    "return_ytd": 8.5,
    "return_1y": 12.5,
    "return_3y": 15.2,
    "return_5y": 18.5,
    "scheme_code": "TOP100G"
  }
}
```

#### Delete fund returns

```
DELETE /api/schemes/{scheme_id}/returns/{date}
```

Response:
```json
{
  "message": "Fund returns deleted successfully"
}
```

### Portfolio Management

#### Get user portfolio

```
GET /api/users/portfolio
```

Response:
```json
[
  {
    "id": 1,
    "user_id": 1,
    "scheme_id": 1,
    "scheme_code": "TOP100G",
    "units": 100,
    "purchase_nav": 100.0,
    "current_nav": 123.45,
    "invested_amount": 10000,
    "current_value": 12345,
    "date_invested": "2023-01-01",
    "last_updated": "2023-01-02T00:00:00Z"
  }
]
```

#### Add to portfolio

```
POST /api/users/portfolio
```

Request body:
```json
{
  "scheme_id": 1,
  "scheme_code": "TOP100G",
  "units": 100,
  "purchase_nav": 100.0,
  "invested_amount": 10000,
  "date_invested": "2023-01-01"
}
```

Response:
```json
{
  "message": "Portfolio entry added successfully",
  "portfolio": {
    "id": 1,
    "user_id": 1,
    "scheme_id": 1,
    "scheme_code": "TOP100G",
    "units": 100,
    "purchase_nav": 100.0,
    "invested_amount": 10000,
    "date_invested": "2023-01-01"
  }
}
```

#### Update portfolio entry

```
PUT /api/users/portfolio/{portfolio_id}
```

Request body:
```json
{
  "units": 150
}
```

Response:
```json
{
  "message": "Portfolio entry updated successfully",
  "portfolio": {
    "id": 1,
    "user_id": 1,
    "scheme_id": 1,
    "scheme_code": "TOP100G",
    "units": 150,
    "purchase_nav": 100.0,
    "current_nav": 123.45,
    "invested_amount": 15000,
    "current_value": 18517.5,
    "date_invested": "2023-01-01",
    "last_updated": "2023-01-03T00:00:00Z"
  }
}
```

#### Delete portfolio entry

```
DELETE /api/users/portfolio/{portfolio_id}
```

Response:
```json
{
  "message": "Portfolio entry deleted successfully"
}
```

### Fund Holdings

#### Create fund holding

```
POST /api/schemes/{scheme_id}/holdings
```

Request body:
```json
{
  "security_name": "HDFC Bank Ltd",
  "isin": "INE040A01034",
  "sector": "Financial Services",
  "asset_type": "Equity",
  "weightage": 9.8,
  "holding_value": 980000000
}
```

Response:
```json
{
  "message": "Fund holding created successfully",
  "holding": {
    "id": 1,
    "scheme_id": 1,
    "security_name": "HDFC Bank Ltd",
    "isin": "INE040A01034",
    "sector": "Financial Services",
    "asset_type": "Equity",
    "weightage": 9.8,
    "holding_value": 980000000,
    "last_updated": "2023-01-01T00:00:00Z"
  }
}
```

#### Get fund holdings

```
GET /api/schemes/{scheme_id}/holdings
```

Response:
```json
[
  {
    "id": 1,
    "scheme_id": 1,
    "security_name": "HDFC Bank Ltd",
    "isin": "INE040A01034",
    "sector": "Financial Services",
    "asset_type": "Equity",
    "weightage": 9.8,
    "holding_value": 980000000,
    "last_updated": "2023-01-01T00:00:00Z"
  }
]
```

#### Get a specific fund holding

```
GET /api/holdings/{holding_id}
```

Response:
```json
{
  "id": 1,
  "scheme_id": 1,
  "security_name": "HDFC Bank Ltd",
  "isin": "INE040A01034",
  "sector": "Financial Services",
  "asset_type": "Equity",
  "weightage": 9.8,
  "holding_value": 980000000,
  "last_updated": "2023-01-01T00:00:00Z"
}
```

#### Update fund holding

```
PUT /api/holdings/{holding_id}
```

Request body:
```json
{
  "weightage": 10.2,
  "holding_value": 1020000000
}
```

Response:
```json
{
  "message": "Fund holding updated successfully",
  "holding": {
    "id": 1,
    "scheme_id": 1,
    "security_name": "HDFC Bank Ltd",
    "isin": "INE040A01034",
    "sector": "Financial Services",
    "asset_type": "Equity",
    "weightage": 10.2,
    "holding_value": 1020000000,
    "last_updated": "2023-01-02T00:00:00Z"
  }
}
```

#### Delete fund holding

```
DELETE /api/holdings/{holding_id}
```

Response:
```json
{
  "message": "Fund holding deleted successfully"
}
```

## External Service Integration

### Hyperverge Integration (KYC Verification)

The API integrates with Hyperverge for KYC verification. This includes:

1. PAN card verification
2. Face matching between selfie and ID
3. Liveness detection

#### Configuration

The Hyperverge integration requires the following environment variables:

- `HYPERVERGE_APP_ID`: Your Hyperverge application ID
- `HYPERVERGE_APP_KEY`: Your Hyperverge application key

#### API Endpoints

The Hyperverge service provides the following functionality:

- Verify ID card (PAN, Aadhaar, etc.)
- Verify face match between selfie and ID
- Verify liveness using video

### BSE Star Integration

The API integrates with BSE Star for:

1. Client registration
2. Bank account registration
3. Mandate registration
4. Purchase transactions
5. Redemption transactions

#### Configuration

The BSE Star integration requires the following environment variables:

- `BSE_STAR_API_KEY`: Your BSE Star API key

#### API Endpoints

The BSE Star service provides the following functionality:

- Register a client with BSE Star
- Register a bank account for a client
- Register a mandate for a client
- Execute purchase transactions
- Execute redemption transactions

## Database Models

The API uses the following main database models:

1. UserInfo: User account details
2. KycDetail: KYC details for users
3. BankRepo/BranchRepo: Repository of banks and branches
4. BankDetail: User bank account details
5. Amc: Asset Management Companies
6. Fund: Mutual funds under an AMC
7. FundScheme: Different schemes under a fund
8. FundSchemeDetail: Details of each scheme
9. FundFactSheet: Detailed factsheet for a fund scheme
10. Returns: Historical returns for a fund scheme
11. FundHolding: Holdings of a fund scheme
12. Portfolio: User's investments in funds

## Security Considerations

### Authentication Security
- JWT tokens expire after 24 hours
- Passwords are hashed using Werkzeug's security functions
- Sensitive routes require authentication via JWT token
- Failed login attempts are logged for security monitoring

### Data Security
- PAN numbers and other sensitive data stored securely
- Input validation enforced on all endpoints
- Database constraints ensure data integrity
- Regular security audits recommended

### API Best Practices
- Use HTTPS for all API calls in production
- Store JWT tokens securely (HTTP-only cookies recommended for web applications)
- Implement rate limiting in production environments
- Refresh tokens periodically
- Never expose tokens in URLs or log files

## Rate Limiting

The API implements rate limiting in production to prevent abuse:
- 100 requests per minute for authenticated users
- 20 requests per minute for unauthenticated users

## Environment Variables

The following environment variables should be configured:

- `DATABASE_URL`: PostgreSQL database connection URL
- `SESSION_SECRET`: Secret key for JWT token generation
- `HYPERVERGE_APP_ID`: Hyperverge application ID
- `HYPERVERGE_APP_KEY`: Hyperverge application key
- `BSE_STAR_API_KEY`: BSE Star API key

## Common Use Cases

### Onboarding a New User
1. Register user account
2. Complete KYC verification
3. Add bank details
4. Register with BSE Star

### Adding a Fund to Portfolio
1. Find fund through AMC listings
2. View fund details and returns
3. Add to portfolio with purchase details

### Tracking Portfolio Performance
1. Retrieve portfolio details
2. View current values and returns
3. Compare with benchmark indices