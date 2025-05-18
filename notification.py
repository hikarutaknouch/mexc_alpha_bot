"""
通知機能モジュール
Discord・LINE・メールなどでの通知送信
"""
import os
import json
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv

# 環境変数読み込み
load_dotenv()
NOTIFICATION_ENABLED = os.getenv("NOTIFICATION_ENABLED", "0") == "1"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
LINE_TOKEN = os.getenv("LINE_NOTIFY_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "0") == "1"
EMAIL_SERVER = os.getenv("EMAIL_SERVER", "")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "")

# ロガー設定
logger = logging.getLogger(__name__)

def send_notification(message, level="info"):
    """
    複数の通知チャネルにメッセージを送信
    
    Args:
        message (str): 送信するメッセージ
        level (str): 通知レベル ('info', 'warning', 'error')
    
    Returns:
        bool: すべての通知が成功したかどうか
    """
    if not NOTIFICATION_ENABLED:
        return True
    
    # タイムスタンプ付きメッセージ
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    
    success = True
    
    # Discord通知
    if DISCORD_WEBHOOK_URL:
        discord_success = send_discord_notification(full_message, level)
        success = success and discord_success
    
    # LINE通知
    if LINE_TOKEN:
        line_success = send_line_notification(full_message)
        success = success and line_success
    
    # Telegram通知
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        telegram_success = send_telegram_notification(full_message)
        success = success and telegram_success
    
    # メール通知（エラーのみ）
    if EMAIL_ENABLED and level == "error":
        email_success = send_email_notification(f"[MEXC BOT ERROR] {message}", full_message)
        success = success and email_success
    
    return success

def send_discord_notification(message, level="info"):
    """
    Discord Webhookを使用して通知を送信
    
    Args:
        message (str): 送信するメッセージ
        level (str): 通知レベル ('info', 'warning', 'error')
    
    Returns:
        bool: 通知が成功したかどうか
    """
    if not DISCORD_WEBHOOK_URL:
        return False
    
    # レベルに応じた色を設定
    colors = {
        "info": 3447003,  # 青
        "warning": 16776960,  # 黄色
        "error": 15158332  # 赤
    }
    color = colors.get(level, colors["info"])
    
    data = {
        "embeds": [{
            "title": "MEXC Bot通知",
            "description": message,
            "color": color
        }]
    }
    
    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json=data,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code >= 400:
            logger.error(f"Discord通知エラー: {response.status_code} {response.text}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Discord通知送信エラー: {e}")
        return False

def send_line_notification(message):
    """
    LINE Notifyを使用して通知を送信
    
    Args:
        message (str): 送信するメッセージ
    
    Returns:
        bool: 通知が成功したかどうか
    """
    if not LINE_TOKEN:
        return False
    
    try:
        response = requests.post(
            "https://notify-api.line.me/api/notify",
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            data={"message": message}
        )
        
        if response.status_code != 200:
            logger.error(f"LINE通知エラー: {response.status_code} {response.text}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"LINE通知送信エラー: {e}")
        return False

def send_telegram_notification(message):
    """
    Telegramを使用して通知を送信
    
    Args:
        message (str): 送信するメッセージ
    
    Returns:
        bool: 通知が成功したかどうか
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        
        response = requests.post(url, data=data)
        
        if response.status_code != 200:
            logger.error(f"Telegram通知エラー: {response.status_code} {response.text}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Telegram通知送信エラー: {e}")
        return False

def send_email_notification(subject, message):
    """
    SMTPを使用してメール通知を送信
    
    Args:
        subject (str): メールの件名
        message (str): メールの本文
    
    Returns:
        bool: 通知が成功したかどうか
    """
    if not EMAIL_ENABLED or not EMAIL_SERVER or not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECIPIENT:
        return False
    
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        # メールの作成
        mail = MIMEMultipart()
        mail["From"] = EMAIL_USER
        mail["To"] = EMAIL_RECIPIENT
        mail["Subject"] = subject
        
        # メール本文の追加
        mail.attach(MIMEText(message, "plain"))
        
        # SMTP接続
        server = smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT)
        server.starttls()  # TLS暗号化
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        
        # メール送信
        server.send_message(mail)
        server.quit()
        
        return True
    except Exception as e:
        logger.error(f"メール通知送信エラー: {e}")
        return False

# テスト用
if __name__ == "__main__":
    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )
    
    # 各通知をテスト
    send_notification("これはinfo通知のテストです", "info")
    send_notification("これはwarning通知のテストです", "warning")
    send_notification("これはerror通知のテストです", "error")