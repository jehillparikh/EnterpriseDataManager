from marshmallow import Schema, fields, validate, ValidationError, validates_schema

class UserSchema(Schema):
    """Schema for validating User data"""
    
    id = fields.String(dump_only=True)
    username = fields.String(
        required=True, 
        validate=validate.Length(min=3, max=64),
        error_messages={"required": "Username is required."}
    )
    email = fields.Email(
        required=True,
        error_messages={"required": "Email is required."}
    )
    password = fields.String(
        load_only=True,  # Only used when loading data (not included in serialized output)
        required=True,
        validate=validate.Length(min=8),
        error_messages={"required": "Password is required."}
    )
    created_at = fields.DateTime(dump_only=True)
    updated_at = fields.DateTime(dump_only=True)
    
    @validates_schema
    def validate_unique_fields(self, data, **kwargs):
        """
        Custom validation to ensure username and email are unique
        This will be implemented in services.py
        """
        pass


class UserUpdateSchema(Schema):
    """Schema for validating User update data"""
    
    username = fields.String(validate=validate.Length(min=3, max=64))
    email = fields.Email()
    password = fields.String(load_only=True, validate=validate.Length(min=8))


class QuestionSchema(Schema):
    """Schema for validating Question data"""
    
    id = fields.String(dump_only=True)
    title = fields.String(
        required=True,
        validate=validate.Length(min=5, max=200),
        error_messages={"required": "Title is required."}
    )
    content = fields.String(
        required=True,
        validate=validate.Length(min=10),
        error_messages={"required": "Content is required."}
    )
    user_id = fields.String(required=True)
    tags = fields.List(fields.String())
    created_at = fields.DateTime(dump_only=True)
    updated_at = fields.DateTime(dump_only=True)


class QuestionUpdateSchema(Schema):
    """Schema for validating Question update data"""
    
    title = fields.String(validate=validate.Length(min=5, max=200))
    content = fields.String(validate=validate.Length(min=10))
    tags = fields.List(fields.String())


# Create schema instances
user_schema = UserSchema()
users_schema = UserSchema(many=True)
user_update_schema = UserUpdateSchema()

question_schema = QuestionSchema()
questions_schema = QuestionSchema(many=True)
question_update_schema = QuestionUpdateSchema()
