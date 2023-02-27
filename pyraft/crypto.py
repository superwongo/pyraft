#!usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: superwong
@project: firebird-sdk
@file: crypto.py
@time: 2020/8/23
"""

import base64
from abc import ABCMeta, abstractmethod
from io import BytesIO
from typing import Optional, Tuple
from enum import Enum

from Crypto.Hash import SHA256
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.Signature import PKCS1_PSS
from Crypto.Random import get_random_bytes
from Crypto.PublicKey import RSA


__all__ = (
    'RSAKeyFormat',
    'AbstractCryptor',
    'AESCryptor',
    'RSACryptor',
    'HybridCryptor',
    'generate_aes_key',
    'generate_private_key',
    'generate_rsa_keys',
    'get_public_key_from_private'
)


class RSAKeyFormat(str, Enum):
    PEM = 'PEM'
    DER = 'DER'
    OpenSSH = 'OpenSSH'


class AbstractCryptor(metaclass=ABCMeta):
    @abstractmethod
    def encrypt(self, raw_text: str) -> str:
        ...

    @abstractmethod
    def decrypt(self, enc_text: str) -> str:
        ...


class AESCryptor(AbstractCryptor):
    """AES加解密"""
    def __init__(self, key: str, mode=AES.MODE_EAX, encoding: str = 'utf8'):
        self._key = base64.b64decode(key.encode(encoding))
        self._mode = mode
        self._encoding = encoding

    def encrypt(self, raw_text: str):
        cipher = AES.new(self._key, self._mode)
        ciphertext, tag = cipher.encrypt_and_digest(raw_text.encode(self._encoding))
        file_out = BytesIO()
        [file_out.write(x) for x in (cipher.nonce, tag, ciphertext)]
        return base64.b64encode(file_out.getvalue()).decode(self._encoding)

    def decrypt(self, enc_text: str):
        file_in = BytesIO(base64.b64decode(enc_text))
        nonce, tag, ciphertext = [file_in.read(x) for x in (16, 16, -1)]

        # let's assume that the key is somehow available again
        cipher = AES.new(self._key, self._mode, nonce)
        raw_text = cipher.decrypt_and_verify(ciphertext, tag)
        return raw_text.decode(self._encoding)


class RSACryptor(AbstractCryptor):
    """RSA加解密"""
    def __init__(
            self,
            private_key: Optional[str] = None,
            public_key: Optional[str] = None,
            passphrase: Optional[str] = None,
            encoding: str = 'utf8'
    ):
        self._private_key = private_key
        self._public_key = public_key
        self._private_key_obj = RSA.import_key(private_key, passphrase=passphrase) if private_key else None
        self._public_key_obj = RSA.import_key(
            public_key or self._private_key_obj.public_key().export_key(), passphrase=passphrase
        )
        self._encoding = encoding

    def encrypt(self, raw_text: str, length: int = 200) -> str:
        """公钥加密
        单次加密串的长度最大为：(key_size/8)-11
        1024bit的证书用100，2048bit的证书用200
        """
        if not self._public_key_obj:
            raise ValueError('加密操作时，public_key不可为空')

        cipher_rsa = PKCS1_OAEP.new(self._public_key_obj)
        raw_text_bytes = raw_text.encode(self._encoding)
        enc_text_list = [cipher_rsa.encrypt(raw_text_bytes[i:i+length]) for i in range(0, len(raw_text_bytes), length)]
        return base64.b64encode(b''.join(enc_text_list)).decode(self._encoding)

    def decrypt(self, enc_text: str, length: int = 256) -> str:
        """私钥解密
        1024bit的证书用128，2048bit的证书用256
        """
        if not self._private_key_obj:
            raise ValueError('解密操作时，private_key不可为空')

        # Decrypt the session key with the private RSA key
        cipher_rsa = PKCS1_OAEP.new(self._private_key_obj)
        enc_text_bytes = base64.b64decode(enc_text)
        raw_text_list = [cipher_rsa.decrypt(enc_text_bytes[i:i+length]) for i in range(0, len(enc_text_bytes), length)]
        return b''.join(raw_text_list).decode(self._encoding)

    def signature(self, text: str) -> str:
        """私钥签名"""
        if not self._private_key_obj:
            raise ValueError('签名操作时，private_key不可为空')
        signer = PKCS1_PSS.new(self._private_key_obj)
        digest = SHA256.new(text.encode(self._encoding))
        sign = signer.sign(digest)
        return base64.b64encode(sign).decode(self._encoding)

    def verify(self, text: str, signature: str) -> bool:
        """公钥验签"""
        if not self._public_key_obj:
            raise ValueError('验签操作时，public_key不可为空')
        verifier = PKCS1_PSS.new(self._public_key_obj)
        digest = SHA256.new(text.encode(self._encoding))
        try:
            verifier.verify(digest, base64.b64decode(signature))
            return True
        except (ValueError, TypeError):
            return False


class HybridCryptor(AbstractCryptor):
    """Hybrid加解密"""
    def __init__(
            self,
            private_key: Optional[str] = None,
            public_key: Optional[str] = None,
            mode: Optional[int] = None,
            passphrase: Optional[str] = None,
            encoding: str = 'utf8'
    ):
        self._private_key = RSA.import_key(private_key, passphrase=passphrase) if private_key else None
        self._public_key = RSA.import_key(
            public_key or self._private_key.public_key().export_key(), passphrase=passphrase
        )
        self._mode = mode or AES.MODE_EAX
        self._encoding = encoding
        self._session_key = get_random_bytes(16)

    def encrypt(self, raw_text: str) -> str:
        if not self._public_key:
            raise ValueError('加密操作时，public_key不可为空')

        cipher_rsa = PKCS1_OAEP.new(self._public_key)
        enc_session_key = cipher_rsa.encrypt(self._session_key)

        cipher_aes = AES.new(self._session_key, self._mode)
        ciphertext, tag = cipher_aes.encrypt_and_digest(raw_text.encode(self._encoding))
        file_out = BytesIO()
        [file_out.write(x) for x in (enc_session_key, cipher_aes.nonce, tag, ciphertext)]
        return base64.b64encode(file_out.getvalue()).decode(self._encoding)

    def decrypt(self, enc_text: str) -> str:
        if not self._private_key:
            raise ValueError('加密操作时，private_key不可为空')

        file_in = BytesIO(base64.b64decode(enc_text))
        enc_session_key, nonce, tag, ciphertext = \
            [file_in.read(x) for x in (self._private_key.size_in_bytes(), 16, 16, -1)]

        # Decrypt the session key with the private RSA key
        cipher_rsa = PKCS1_OAEP.new(self._private_key)
        session_key = cipher_rsa.decrypt(enc_session_key)

        # Decrypt the data with the AES session key
        cipher_aes = AES.new(session_key, self._mode, nonce)
        return cipher_aes.decrypt_and_verify(ciphertext, tag).decode(self._encoding)


def generate_aes_key(num: int = 16, encoding: str = 'utf8') -> str:
    return base64.b64encode(get_random_bytes(num)).decode(encoding)


def generate_private_key(
        fmt: Optional[RSAKeyFormat] = None,
        passphrase: Optional[str] = None,
        encoding: str = 'utf8'
) -> str:
    fmt = fmt or RSAKeyFormat.PEM
    key = RSA.generate(2048)
    encrypted_key = key.export_key(format=fmt.value, passphrase=passphrase, pkcs=8, protection="scryptAndAES128-CBC")
    return encrypted_key.decode(encoding)


def generate_rsa_keys(
        fmt: Optional[RSAKeyFormat] = None,
        passphrase: Optional[str] = None,
        encoding: str = 'utf8'
) -> Tuple[str, str]:
    fmt = fmt or RSAKeyFormat.PEM
    key = RSA.generate(2048)
    encrypted_key = key.export_key(format=fmt.value, passphrase=passphrase, pkcs=8, protection="scryptAndAES128-CBC")
    return encrypted_key.decode('utf8'), key.publickey().export_key(format=fmt.value).decode(encoding)


def get_public_key_from_private(
        private_key: str,
        passphrase: Optional[str] = None,
        to_fmt: Optional[RSAKeyFormat] = None,
        encoding: str = 'utf8'
) -> str:
    to_fmt = to_fmt or RSAKeyFormat.PEM
    pk_obj = RSA.import_key(private_key, passphrase=passphrase)
    key = pk_obj.public_key().export_key(format=to_fmt.value)
    return key.decode(encoding)


def test_aes_encrypt():
    k = generate_aes_key()
    print(f'>>>>>k: {k}')
    aes = AESCryptor(k)
    raw_text = '12345'
    enc_text = aes.encrypt(raw_text)
    print(f'>>>>>enc_text: {enc_text}')
    raw_text = aes.decrypt(enc_text)
    print(f'>>>>>raw_text: {raw_text}')


def test_rsa_encrypt():
    private_key, public_key = generate_rsa_keys()
    print(f'>>>>>private_key: {private_key}')
    print(f'>>>>>public_key: {public_key}')
    raw_text = '123452222222222222222222222222222222222222222222222222222222222222222222222'
    rsa = RSACryptor(private_key, public_key)
    enc_text = rsa.encrypt(raw_text)
    print(f'>>>>>secret_text: {enc_text}')
    raw_text = rsa.decrypt(enc_text)
    print(f'>>>>>raw_text: {raw_text}')


def test_rsa_signature():
    private_key, public_key = generate_rsa_keys()
    print(f'>>>>>private_key: {private_key}')
    print(f'>>>>>public_key: {public_key}')
    text = '123452222222222222222222222222222222222222222222222222222222222222222222222'
    rsa = RSACryptor(private_key, public_key)
    signature = rsa.signature(text)
    print(f'>>>>>signature: {signature}')
    is_verify = rsa.verify(text, signature)
    print(f'>>>>>is_verify: {is_verify}')


def test_hybrid_encrypt():
    passphrase = 'abcde'
    private_key, public_key = generate_rsa_keys(passphrase=passphrase)
    raw_text = '123452222222222222222222222222222222222222222222222222222222222222222222222'
    hybrid = HybridCryptor(private_key, public_key, passphrase=passphrase)
    enc_text = hybrid.encrypt(raw_text)
    print(f'>>>>>secret_text: {enc_text}')
    raw_text = hybrid.decrypt(enc_text)
    print(f'>>>>>raw_text: {raw_text}')


def test_private_to_public_key():
    private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEA0/N/HIK4Ui5s5Hy/wTCGmXz0wlrhaLxm6J5gKkXcDF6YNCvz
g29vt2SsTqYmt8v/83Sci8PP7UJfvMqW9+BlXLG+4aKQeTqz7QIStKc97G1K5SGw
YIfjln5TuRMYOZzEeFoPMl9jB/AYDcw2w14q1mNkYn+sSkGGzKit7Page/pi2v3J
6hmz4tTGCN9imbSS6es0OFvZLW/1VPkEo0eOVY91zbaYyWlebEe7sFtH1cNhABML
Pbevk0OaQdppMqFOBt+d9K6CUXTxrwWH3oITt3FGsEGg66zqdWYChYCUNKhUEv4V
XPijxkcTrUPSRjuhHJxd+k6U0lKFCiF+aJInWwIDAQABAoIBAD/VhCD4Fb9akB04
nR/4F3G+upCWgnDi3TOOD3Rr2dw32StNWWfqdgEL5wFA9URBwnJ2WneVn9fbN65K
bbF1+dEzD0Qxi8WXMn1dNHkILZQ5Tmc4AEDybE70+Q4yfAdN+bYtmYoYRGEtpHDz
YkLw54HuKVupDCDJH2ogG1HU7Xyo3jFOvyIEOkFJVwzVNAT3VFyp6bFSZsWc8IEx
6SuEh9RhGODO3l2tPk0xaTBuQQ3fH2Z5TUraNO5HT/PY80i/UXhIlyvoc+OyXQ3q
T9eQA2b9I3hnu07v/c6QSfXPFp43xizbQQn8oRDmMN7JrJlZqWoJ4qBFYUroGQvZ
OnvQrpECgYEA6acLlNljklv2jW1b+6BZTsYMW+F1lXuoF4LpZp8pTVEsGKbBfTmy
xWgklMqrmK6/qbpOnM9XciIW1virANDx9R0OCAT6rd3L6Wpcm4RPQt2zey7eB3/f
Z9Vk6UCZrrJdJHKssAk5akV2uJKW7vn4gCtmRqHLQzuXStW8nTlU01cCgYEA6DkY
kQm3S3OthxletlkBSiiCmTY9cmlwNt7EIK6tI5BLo3nqj5VE+0vj+8mMfZRtJQ4X
Y9HckoS0UkBGr+Ao311T8LgeShzjab8T+uFYx3kpE3fnytPoh6Zvpbtho18Wmddy
jFm3qZhgev9aiXR90hAqpKNMH9vD3a2Uz+Uj7Z0CgYAFtE5UE/qaWCRsz36vZfrI
MY2bsKVuQNaZvFh2aOxZNuIct/WBWSdEKoa6GziVQa7sNeA1c9puruZf2TJTksZV
daAiEf0MwZ141V/tbA2DVGZNW5eQF08c+di82RbnbPNZMHOG2LLOJWQAnZORwgiP
GIAmuUx9F7dZ3F3+eKfZgQKBgETAmrheIwlQr0B/T143Tb1x7vCDcX/NHa7xFyoB
7TrJPsugjcSC0hCAcHgcPN71Mbc1y9D+0goDBgwQAZ2ShSdUT0TGjhktIlstejHd
w9YS1glwh4yyXnbC3O05Y/flcCCN+9j3uvuYJIBl1504gwPNS4LA4gnQm9WmzZcb
ZR61AoGAC43faGNyQFNzKayzrSMZPvcsgZbabb6PdC1BrQtXpqnMjBHVOuFN/r5H
pVAoDGFbbDOMH79EkJ3EVkr/ljElNP0Hjq+EOGVGX0AbXUf8tQAVfRPwaV3p6NYY
8iW0f1oz91QZ27OG8BXDOrWT2ax4uLXWAb4BvEaXmU7X4XlwAZ8=
-----END RSA PRIVATE KEY-----"""
    pk = get_public_key_from_private(private_key, to_fmt=RSAKeyFormat.OpenSSH)
    print(f'>>>>>public_key: {pk}')


if __name__ == '__main__':
    # test_aes_encrypt()
    # test_rsa_encrypt()
    test_rsa_signature()
    # test_hybrid_encrypt()
    # test_private_to_public_key()
