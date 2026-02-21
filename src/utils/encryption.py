"""
Encryption utilities for secure credential storage
"""
import os
import platform
import getpass
import base64
import logging
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from pathlib import Path

logger = logging.getLogger(__name__)

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
            password = f"ExpiryTrack_{platform.node() or 'default'}_2024".encode()
            key = base64.urlsafe_b64encode(kdf.derive(password))

            # Save key
            with open(self.key_file, 'wb') as f:
                f.write(key)

            # Set file permissions (Windows compatible)
            try:
                import stat
                os.chmod(self.key_file, stat.S_IRUSR | stat.S_IWUSR)
            except Exception:
                pass  # Windows may not support chmod

            return key

    def _get_machine_salt(self) -> bytes:
        """Generate machine-specific salt"""
        hostname = platform.node() or 'unknown'
        try:
            username = getpass.getuser()
        except Exception:
            username = 'user'
        machine_id = f"{hostname}_{username}"
        return machine_id.encode()[:16].ljust(16, b'0')  # Ensure 16 bytes

    def encrypt(self, plaintext: str) -> str:
        """Encrypt plaintext string (Fernet output is already base64-safe)"""
        if not plaintext:
            return ""
        return self.cipher.encrypt(plaintext.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        """Decrypt ciphertext string. Supports both new (single base64) and legacy (double base64) formats."""
        if not ciphertext:
            return ""
        # Try new format first (direct Fernet token)
        try:
            return self.cipher.decrypt(ciphertext.encode()).decode()
        except Exception:
            pass
        # Fall back to legacy double-base64 format
        try:
            encrypted_bytes = base64.urlsafe_b64decode(ciphertext.encode())
            return self.cipher.decrypt(encrypted_bytes).decode()
        except Exception as e:
            logger.warning(f"Decryption failed: {e}")
            return ""

# Singleton instance
encryption = CredentialEncryption()