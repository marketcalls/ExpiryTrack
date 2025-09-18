"""
Encryption utilities for secure credential storage
"""
import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from pathlib import Path

class CredentialEncryption:
    """Handle encryption/decryption of sensitive credentials"""

    def __init__(self):
        self.key_file = Path.home() / '.expirytrack' / '.key'
        self.key = self._get_or_create_key()
        self.cipher = Fernet(self.key)

    def _get_or_create_key(self) -> bytes:
        """Get existing key or create new one"""
        # Create directory if doesn't exist
        self.key_file.parent.mkdir(exist_ok=True, parents=True)

        if self.key_file.exists():
            # Load existing key
            with open(self.key_file, 'rb') as f:
                return f.read()
        else:
            # Generate new key using machine-specific salt
            salt = self._get_machine_salt()
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )

            # Use combination of machine ID and fixed password
            password = f"ExpiryTrack_{os.environ.get('COMPUTERNAME', 'default')}_2024".encode()
            key = base64.urlsafe_b64encode(kdf.derive(password))

            # Save key
            with open(self.key_file, 'wb') as f:
                f.write(key)

            # Set file permissions (Windows compatible)
            try:
                import stat
                os.chmod(self.key_file, stat.S_IRUSR | stat.S_IWUSR)
            except:
                pass  # Windows may not support chmod

            return key

    def _get_machine_salt(self) -> bytes:
        """Generate machine-specific salt"""
        machine_id = f"{os.environ.get('COMPUTERNAME', 'unknown')}_{os.environ.get('USERNAME', 'user')}"
        return machine_id.encode()[:16].ljust(16, b'0')  # Ensure 16 bytes

    def encrypt(self, plaintext: str) -> str:
        """Encrypt plaintext string"""
        if not plaintext:
            return ""
        encrypted_bytes = self.cipher.encrypt(plaintext.encode())
        return base64.urlsafe_b64encode(encrypted_bytes).decode()

    def decrypt(self, ciphertext: str) -> str:
        """Decrypt ciphertext string"""
        if not ciphertext:
            return ""
        try:
            encrypted_bytes = base64.urlsafe_b64decode(ciphertext.encode())
            decrypted_bytes = self.cipher.decrypt(encrypted_bytes)
            return decrypted_bytes.decode()
        except Exception:
            return ""  # Return empty string if decryption fails

# Singleton instance
encryption = CredentialEncryption()