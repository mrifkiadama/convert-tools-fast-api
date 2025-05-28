from app.utils.security import get_password_hash, verify_password

plain_password = "your_password"
hashed_password = get_password_hash(plain_password)
print(f"Plain: {plain_password}")
print(f"Hashed: {hashed_password}")
assert verify_password(plain_password, hashed_password), "Password verification failed"
print("Password verified correctly")