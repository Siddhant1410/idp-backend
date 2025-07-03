from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding as sym_padding
import base64

SECRET_KEY = b'A7x!m3ZqP9t#F6vLb2r@X4hKd8WcY1eB'  # Must be exactly 32 bytes

def fix_base64_padding(s: str) -> str:
    return s + '=' * (-len(s) % 4)

def decrypt_password(encrypted_base64: str, secret_key: bytes) -> str:
    try:
        # Fix base64 padding
        encrypted_base64 = fix_base64_padding(encrypted_base64)
        raw = base64.b64decode(encrypted_base64)

        iv = raw[:16]
        ciphertext = raw[16:]

        cipher = Cipher(algorithms.AES(secret_key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        # Remove PKCS7 padding
        unpadder = sym_padding.PKCS7(128).unpadder()  # Block size = 128 bits = 16 bytes
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

        return plaintext.decode('utf-8')
    except Exception as e:
        print("❌ Failed to decrypt:", e)
        return None

# Example usage
encrypted = "8aDicgDEEtA7J6oCr/5EAWNylraHBoYzWoP3FOZhzlLtZfAoyqN3Y1gM11NaIZB7"
print("Decrypted password:", decrypt_password(encrypted, SECRET_KEY))
