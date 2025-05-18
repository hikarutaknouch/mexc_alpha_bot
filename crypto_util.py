"""
APIキーの暗号化・復号化ユーティリティ - 完全版
セキュリティ強化と例外処理の改善
"""
import os
import base64
import hashlib
import getpass
import logging
import json
import time
from typing import Tuple, Optional, Dict, Any
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from dotenv import load_dotenv, set_key

# ロギング設定
logger = logging.getLogger(__name__)

# 環境変数読み込み
load_dotenv()

# 定数設定
ITERATIONS = int(os.getenv("CRYPTO_ITERATIONS", "100000"))  # KDFのイテレーション回数
SALT_FILE = os.getenv("SALT_FILE", ".crypto_salt")          # 塩を保存するファイル
KEY_CACHE_TIMEOUT = int(os.getenv("KEY_CACHE_TIMEOUT", "3600"))  # キャッシュのタイムアウト（秒）

# キーキャッシュ（セッション中に再入力を避けるため）
key_cache = {
    "master_key": None,
    "timestamp": 0
}

def generate_key(password: str, salt: bytes = None) -> Tuple[bytes, bytes]:
    """
    パスワードからキー導出関数を使用して暗号化キーを生成
    
    Args:
        password (str): マスターパスワード
        salt (bytes, optional): 既存の塩。Noneの場合は新しく生成。
    
    Returns:
        Tuple[bytes, bytes]: (生成されたキー, 使用された塩)
    """
    if salt is None:
        salt = os.urandom(16)
    
    try:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=ITERATIONS,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key, salt
    except Exception as e:
        logger.error(f"キー生成エラー: {e}", exc_info=True)
        raise ValueError(f"キー生成に失敗しました: {e}")

def encrypt_api_key(api_key: str, master_password: str) -> Tuple[bytes, bytes]:
    """
    APIキーを暗号化
    
    Args:
        api_key (str): 暗号化するAPIキー
        master_password (str): マスターパスワード
    
    Returns:
        Tuple[bytes, bytes]: (暗号化されたキー, 使用された塩)
    """
    try:
        key, salt = generate_key(master_password)
        f = Fernet(key)
        encrypted_key = f.encrypt(api_key.encode())
        return encrypted_key, salt
    except Exception as e:
        logger.error(f"暗号化エラー: {e}", exc_info=True)
        raise ValueError(f"暗号化に失敗しました: {e}")

def decrypt_api_key(encrypted_key: bytes, salt: bytes, master_password: str) -> str:
    """
    暗号化されたAPIキーを復号
    
    Args:
        encrypted_key (bytes): 暗号化されたAPIキー
        salt (bytes): 使用された塩
        master_password (str): マスターパスワード
    
    Returns:
        str: 復号されたAPIキー
    """
    try:
        key, _ = generate_key(master_password, salt)
        f = Fernet(key)
        decrypted_key = f.decrypt(encrypted_key).decode()
        return decrypted_key
    except InvalidToken:
        logger.error("パスワードが間違っています。復号に失敗しました。")
        raise ValueError("パスワードが間違っています。復号に失敗しました。")
    except Exception as e:
        logger.error(f"復号エラー: {e}", exc_info=True)
        raise ValueError(f"復号に失敗しました: {e}")

def save_salt_to_file(salt: bytes, salt_type: str = "default") -> bool:
    """
    塩をファイルに保存
    
    Args:
        salt (bytes): 保存する塩
        salt_type (str): 塩の種類を識別するキー
    
    Returns:
        bool: 保存が成功したかどうか
    """
    try:
        # 既存ファイルの読み込み
        salt_data = {}
        salt_file = Path(SALT_FILE)
        
        if salt_file.exists():
            with open(salt_file, 'r') as f:
                try:
                    salt_data = json.load(f)
                except json.JSONDecodeError:
                    # ファイルが壊れている場合は新規作成
                    salt_data = {}
        
        # 塩を更新
        salt_data[salt_type] = base64.b64encode(salt).decode('utf-8')
        
        # 保存
        with open(salt_file, 'w') as f:
            json.dump(salt_data, f)
        
        # ファイルのパーミッションを制限（Unixシステムのみ）
        if os.name != 'nt':  # Windows以外
            os.chmod(salt_file, 0o600)  # ユーザーのみに読み書き権限
            
        return True
    except Exception as e:
        logger.error(f"塩保存エラー: {e}", exc_info=True)
        return False

def load_salt_from_file(salt_type: str = "default") -> Optional[bytes]:
    """
    ファイルから塩を読み込み
    
    Args:
        salt_type (str): 塩の種類を識別するキー
    
    Returns:
        Optional[bytes]: 読み込まれた塩またはNone
    """
    try:
        salt_file = Path(SALT_FILE)
        
        if not salt_file.exists():
            return None
        
        with open(salt_file, 'r') as f:
            salt_data = json.load(f)
        
        if salt_type not in salt_data:
            return None
        
        return base64.b64decode(salt_data[salt_type])
    except Exception as e:
        logger.error(f"塩読み込みエラー: {e}", exc_info=True)
        return None

def setup_encrypted_keys() -> bool:
    """
    APIキーを暗号化して.envファイルに保存
    
    Returns:
        bool: セットアップが成功したかどうか
    """
    print("\n===== APIキー暗号化セットアップ =====")
    
    load_dotenv()
    
    # すでに暗号化済みかチェック
    if os.getenv("ENCRYPTED_KEYS") == "1":
        print("キーはすでに暗号化されています。再暗号化するには先に.envファイルから ENCRYPTED_KEYS を削除してください。")
        return False
    
    # 平文のAPIキーを取得
    mexc_key = os.getenv("MEXC_KEY")
    mexc_secret = os.getenv("MEXC_SECRET")
    
    if not mexc_key or not mexc_secret:
        print("APIキーが設定されていません。先に平文のAPIキーを.envに設定してください。")
        print("例: MEXC_KEY=your_api_key\nMEXC_SECRET=your_api_secret")
        return False
    
    # パスワード強度チェック
    def check_password_strength(password):
        if len(password) < 8:
            return False, "パスワードは8文字以上にしてください"
        
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(not c.isalnum() for c in password)
        
        if sum([has_upper, has_lower, has_digit, has_special]) < 3:
            return False, "パスワードは大文字、小文字、数字、特殊文字のうち3種類以上を含める必要があります"
        
        return True, "パスワードの強度は十分です"
    
    # マスターパスワードの入力
    while True:
        master_password = getpass.getpass("暗号化用のマスターパスワードを入力してください: ")
        
        # パスワード強度チェック
        is_strong, message = check_password_strength(master_password)
        if not is_strong:
            print(f"パスワードが弱すぎます: {message}")
            continue
        
        confirm_password = getpass.getpass("パスワードを再入力: ")
        
        if master_password != confirm_password:
            print("パスワードが一致しません。もう一度お試しください。")
        else:
            break
    
    try:
        # APIキーの暗号化
        print("APIキーを暗号化中...")
        encrypted_key, key_salt = encrypt_api_key(mexc_key, master_password)
        encrypted_secret, secret_salt = encrypt_api_key(mexc_secret, master_password)
        
        # 塩をファイルに保存（オプション）
        use_env_for_salt = input("塩を.envファイルに保存しますか？(推奨: n) [y/N]: ").lower() == 'y'
        
        if use_env_for_salt:
            # 暗号化されたキーと塩を.envファイルに保存
            set_key(".env", "ENCRYPTED_KEYS", "1")
            set_key(".env", "ENCRYPTED_MEXC_KEY", encrypted_key.decode())
            set_key(".env", "ENCRYPTED_MEXC_SECRET", encrypted_secret.decode())
            set_key(".env", "MEXC_KEY_SALT", base64.b64encode(key_salt).decode())
            set_key(".env", "MEXC_SECRET_SALT", base64.b64encode(secret_salt).decode())
        else:
            # 暗号化されたキーを.envに、塩を別ファイルに保存
            set_key(".env", "ENCRYPTED_KEYS", "1")
            set_key(".env", "ENCRYPTED_MEXC_KEY", encrypted_key.decode())
            set_key(".env", "ENCRYPTED_MEXC_SECRET", encrypted_secret.decode())
            
            # 塩を別ファイルに保存
            save_salt_to_file(key_salt, "mexc_key")
            save_salt_to_file(secret_salt, "mexc_secret")
            print(f"塩を {SALT_FILE} に保存しました。このファイルを安全に保管してください。")
        
        # 平文のキーを.envから削除
        set_key(".env", "MEXC_KEY", "")
        set_key(".env", "MEXC_SECRET", "")
        
        print("\nAPIキーが暗号化され、安全に保存されました。")
        print("ボットを起動する際にマスターパスワードの入力が必要になります。")
        
        return True
    
    except Exception as e:
        print(f"暗号化処理中にエラーが発生しました: {e}")
        return False

def get_cached_master_key() -> Optional[bytes]:
    """
    キャッシュからマスターキーを取得（有効期限内の場合）
    
    Returns:
        Optional[bytes]: マスターキーまたはNone
    """
    now = time.time()
    
    if key_cache["master_key"] and (now - key_cache["timestamp"]) < KEY_CACHE_TIMEOUT:
        return key_cache["master_key"]
        
    return None

def set_cached_master_key(master_key: bytes) -> None:
    """
    マスターキーをキャッシュに保存
    
    Args:
        master_key (bytes): 保存するマスターキー
    """
    key_cache["master_key"] = master_key
    key_cache["timestamp"] = time.time()

def decrypt_and_load_keys(master_password: str = None) -> Tuple[Optional[str], Optional[str]]:
    """
    暗号化されたAPIキーを復号して返す
    
    Args:
        master_password (str, optional): マスターパスワード。Noneの場合は入力を求める。
    
    Returns:
        Tuple[Optional[str], Optional[str]]: (APIキー, APIシークレット) または (None, None)
    """
    load_dotenv()
    
    # 暗号化済みかチェック
    if os.getenv("ENCRYPTED_KEYS") != "1":
        # 暗号化されていない場合は平文のキーを返す
        return os.getenv("MEXC_KEY"), os.getenv("MEXC_SECRET")
    
    # キャッシュからマスターキーを取得
    cached_key = get_cached_master_key()
    if cached_key:
        try:
            # 暗号化されたキーを取得
            encrypted_key = os.getenv("ENCRYPTED_MEXC_KEY").encode()
            encrypted_secret = os.getenv("ENCRYPTED_MEXC_SECRET").encode()
            
            # .envから塩を取得するか、ファイルから読み込む
            if os.getenv("MEXC_KEY_SALT") and os.getenv("MEXC_SECRET_SALT"):
                key_salt = base64.b64decode(os.getenv("MEXC_KEY_SALT"))
                secret_salt = base64.b64decode(os.getenv("MEXC_SECRET_SALT"))
            else:
                key_salt = load_salt_from_file("mexc_key")
                secret_salt = load_salt_from_file("mexc_secret")
                
                if not key_salt or not secret_salt:
                    logger.error("塩が見つかりません。暗号化の設定を確認してください。")
                    return None, None
            
            # キーの復号
            f_key = Fernet(cached_key)
            mexc_key = f_key.decrypt(encrypted_key).decode()
            mexc_secret = f_key.decrypt(encrypted_secret).decode()
            
            return mexc_key, mexc_secret
        except Exception as e:
            logger.error(f"キャッシュからの復号失敗: {e}", exc_info=True)
            # キャッシュをクリア
            key_cache["master_key"] = None
            key_cache["timestamp"] = 0
    
    # マスターパスワードの入力
    if master_password is None:
        try:
            master_password = getpass.getpass("APIキー復号用のマスターパスワードを入力してください: ")
        except (KeyboardInterrupt, EOFError):
            logger.error("パスワード入力がキャンセルされました")
            return None, None
    
    try:
        # 暗号化されたキーを取得
        encrypted_key = os.getenv("ENCRYPTED_MEXC_KEY").encode()
        encrypted_secret = os.getenv("ENCRYPTED_MEXC_SECRET").encode()
        
        # .envから塩を取得するか、ファイルから読み込む
        if os.getenv("MEXC_KEY_SALT") and os.getenv("MEXC_SECRET_SALT"):
            key_salt = base64.b64decode(os.getenv("MEXC_KEY_SALT"))
            secret_salt = base64.b64decode(os.getenv("MEXC_SECRET_SALT"))
        else:
            key_salt = load_salt_from_file("mexc_key")
            secret_salt = load_salt_from_file("mexc_secret")
            
            if not key_salt or not secret_salt:
                logger.error("塩が見つかりません。暗号化の設定を確認してください。")
                return None, None
        
        # 復号用のマスターキーを生成してキャッシュ
        master_key, _ = generate_key(master_password, key_salt)
        set_cached_master_key(master_key)
        
        # キーの復号
        mexc_key = decrypt_api_key(encrypted_key, key_salt, master_password)
        mexc_secret = decrypt_api_key(encrypted_secret, secret_salt, master_password)
        
        return mexc_key, mexc_secret
        
    except ValueError as e:
        # パスワードが間違っている場合など
        logger.error(f"復号失敗: {e}")
        print(f"復号失敗: {e}")
        return None, None
        
    except Exception as e:
        logger.error(f"予期せぬエラー: {e}", exc_info=True)
        print(f"予期せぬエラー: {e}")
        return None, None

def encrypt_sensitive_data(data: str, master_password: str = None) -> Dict[str, Any]:
    """
    任意の機密データを暗号化
    
    Args:
        data (str): 暗号化するデータ
        master_password (str, optional): マスターパスワード。Noneの場合はキャッシュまたは入力を使用。
    
    Returns:
        Dict[str, Any]: 暗号化されたデータと関連情報を含む辞書
    """
    # マスターパスワードの取得または入力
    if not master_password:
        cached_key = get_cached_master_key()
        if not cached_key:
            master_password = getpass.getpass("暗号化用のマスターパスワードを入力してください: ")
    
    try:
        # データの暗号化
        encrypted_data, salt = encrypt_api_key(data, master_password)
        
        return {
            "encrypted_data": base64.b64encode(encrypted_data).decode(),
            "salt": base64.b64encode(salt).decode(),
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"データ暗号化エラー: {e}", exc_info=True)
        raise ValueError(f"データの暗号化に失敗しました: {e}")

def decrypt_sensitive_data(encrypted_data: Dict[str, Any], master_password: str = None) -> str:
    """
    暗号化された機密データを復号
    
    Args:
        encrypted_data (Dict[str, Any]): 暗号化されたデータと関連情報を含む辞書
        master_password (str, optional): マスターパスワード。Noneの場合はキャッシュまたは入力を使用。
    
    Returns:
        str: 復号されたデータ
    """
    # マスターパスワードの取得または入力
    if not master_password:
        cached_key = get_cached_master_key()
        if not cached_key:
            master_password = getpass.getpass("復号用のマスターパスワードを入力してください: ")
    
    try:
        # データの復号
        encrypted_bytes = base64.b64decode(encrypted_data["encrypted_data"])
        salt = base64.b64decode(encrypted_data["salt"])
        
        decrypted_data = decrypt_api_key(encrypted_bytes, salt, master_password)
        return decrypted_data
        
    except Exception as e:
        logger.error(f"データ復号エラー: {e}", exc_info=True)
        raise ValueError(f"データの復号に失敗しました: {e}")

# スクリプトを直接実行した場合、セットアップを行う
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )
    
    setup_encrypted_keys()