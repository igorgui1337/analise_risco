#!/usr/bin/env python3
"""Baixa anexos .xlsx de uma conta Gmail via IMAP."""
import os
import imaplib
import email
from email.header import decode_header
from email.message import Message
from datetime import datetime

# Configurações - use variáveis de ambiente ou edite os valores abaixo
EMAIL_ADDRESS = os.environ.get("EMAIL_ADDRESS", "igor.guifreitas@gmail.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "ttjt ikdc ldqt eoew")
IMAP_SERVER = os.environ.get("IMAP_SERVER", "imap.gmail.com")
IMAP_PORT = int(os.environ.get("IMAP_PORT", 993))
SAVE_DIR = os.environ.get("SAVE_DIR", "planilhas_baixadas")
# Filtros de busca
TARGET_SENDER = os.environ.get("TARGET_SENDER", "klaytonsantos@startbetgames.com")
TARGET_SUBJECT = os.environ.get(
    "TARGET_SUBJECT", "Relatório de Alerta de Risco"
)


def connect() -> imaplib.IMAP4_SSL:
    """Conecta ao servidor IMAP do Gmail."""
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    return mail


def decode_mime_words(raw: str) -> str:
    """Decodifica cabeçalhos potencialmente codificados."""
    parts = decode_header(raw)
    decoded = []
    for part, enc in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(enc or "utf-8", errors="ignore"))
        else:
            decoded.append(part)
    return "".join(decoded)


def save_attachment(part: Message) -> None:
    """Salva o anexo se for um arquivo .xlsx."""
    filename = part.get_filename()
    if not filename:
        return
    filename = decode_mime_words(filename)
    if not filename.lower().endswith(".xlsx"):
        return
    os.makedirs(SAVE_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(SAVE_DIR, f"{timestamp}_{filename}")
    with open(out_path, "wb") as f:
        f.write(part.get_payload(decode=True))
    print(f"Salvo: {out_path}")


def download_all_xlsx(mail: imaplib.IMAP4_SSL) -> None:
    """Percorre e-mails filtrando por remetente e assunto e baixa anexos .xlsx."""
    mail.select("INBOX")
    search_criteria = f'(FROM "{TARGET_SENDER}" SUBJECT "{TARGET_SUBJECT}")'
    status, data = mail.search("UTF-8", search_criteria)
    if status != "OK":
        print("Falha ao buscar e-mails")
        return
    for num in data[0].split():
        status, msg_data = mail.fetch(num, "(RFC822)")
        if status != "OK":
            continue
        msg = email.message_from_bytes(msg_data[0][1])
        from_addr = email.utils.parseaddr(msg.get("From"))[1].lower()
        subject = decode_mime_words(msg.get("Subject", ""))
        if from_addr != TARGET_SENDER.lower() or subject != TARGET_SUBJECT:
            continue
        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                save_attachment(part)


def main():
    mail = connect()
    try:
        download_all_xlsx(mail)
    finally:
        mail.logout()


if __name__ == "__main__":
    main()
