# generate_token.py

from jose import jwt
import os
from dotenv import load_dotenv

load_dotenv()  # Load .env file

# Use the secret from your .env
JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-jwt")

# Define the payload (you can add more fields)
payload = {
    "sub": "user_id_123",   # subject / user ID
    "role": "admin"         # example claim
}

# Encode the token
token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")

print("\n✅ Your JWT token:")
print(token)
print("\n🔑 Use this in Swagger as:")
print(f"Bearer {token}")

