import sqlite3
import imaplib
import email
import uuid
import datetime
import json
import email.header
import time
from dateutil.relativedelta import relativedelta

def fetch_emails_by_month(mail, cursor, conn, folder_name, start_date, end_date):
    while start_date <= end_date:
        next_month = start_date + relativedelta(months=1)
        search_criteria = f'(SINCE {start_date.strftime("%d-%b-%Y")} BEFORE {next_month.strftime("%d-%b-%Y")})'
        save_emails_from_folder(folder_name, cursor, mail, conn, search_criteria)
        start_date = next_month
        
def fetch_email_with_retry(mail, num):
    max_attempts = 10  # Maximum attempts
    attempt = 1

    while attempt <= max_attempts:
        result, raw_email = mail.fetch(num, '(RFC822)')
        if result == 'OK' and raw_email:
            raw_email_string = raw_email[0][1]
            email_message = email.message_from_bytes(raw_email_string)
            return email_message  # Success
        else:
            print(f"Error fetching email {num}: {raw_email}")

        if attempt < max_attempts:
            print(f"Attempt {attempt} failed. Retrying in 5 seconds...")
            time.sleep(5)  # Waiting 5 seconds
            attempt += 1
        else:
            print("Max attempts reached. Exiting...")
            return None  # Maximum attempts reached

def save_emails_from_folder(folder_name, cursor, mail, conn, search_criteria):

    mail.select(folder_name)
    result, data = mail.search(None, search_criteria)
    print(f'Saving emails from {folder_name}')

    saved_count = 0
    for num in data[0].split():
        if num.strip():
            email_message = fetch_email_with_retry(mail, num)
            raw_email_string = email_message.as_bytes()

            if 'Message-ID' in email_message:
                message_id = email_message['Message-ID']
            else:
                message_id = str(uuid.uuid4()) + '@nomessageid.com' #nomessageid.com for emails without messageid

            #sender
            sender_header = email_message['From']
            decoded_sender = email.header.decode_header(sender_header)
            sender = ''.join([part[0].decode(part[1] or 'utf-8') if isinstance(part[0], bytes) else str(part[0]) for part in decoded_sender])
            #subject
            subject_header = email_message['Subject']
            decoded_subject = email.header.make_header(email.header.decode_header(subject_header)) if subject_header else 'No subject'
            subject = str(decoded_subject)
            #date
            date = email.utils.parsedate_to_datetime(email_message['Date'])

            cursor.execute("SELECT COUNT(*) FROM mainmails WHERE sender = ? AND subject = ? AND date = ? AND messageid = ?", (sender, subject, date, message_id))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO mainmails (sender, subject, date, body, messageid) VALUES (?, ?, ?, ?, ?)", (sender, subject, date, sqlite3.Binary(raw_email_string), message_id))
                conn.commit()
                saved_count += 1

                if saved_count % 10 == 0:
                    print(date)
                    print(f"{saved_count} saved")
            #else:
            #    print("Письмо уже существует в базе данных.")
            #    conn.close()  # Закрываем соединение с базой данных
            #    mail.close()  # Закрываем соединение с почтовым сервером
            #    mail.logout()  # Выходим из почтового аккаунта
            #    exit()  # Останавливаем выполнение программы                    

def get_ymails():
    conn = None
    mail = None
    imap_server = 'imap.mail.yahoo.com'
    imap_port = 993
    #try: 
    config = load_config()
    if config:
        username = config.get('login')
        password = config.get('password')

    mail = imaplib.IMAP4_SSL(imap_server, imap_port)
    mail.login(username, password)

    # last email in database
    conn = sqlite3.connect('ymails.db')
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM mainmails")
    max_date = cursor.fetchone()[0]
    
    if max_date:
        # Используем дату последнего письма плюс один день как дату начала поиска
        max_date = datetime.datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S%z')
        date_since = max_date + datetime.timedelta(seconds=1)
    else:
        # Если база данных пуста, начинаем с определенной даты
        print('Date was empty')
        date_since = datetime.datetime(2022, 12, 1, tzinfo=datetime.timezone.utc)

    current_datetime = datetime.datetime.now(datetime.timezone.utc)
    
    fetch_emails_by_month(mail, cursor, conn, 'inbox', date_since, current_datetime)
    fetch_emails_by_month(mail, cursor, conn, 'linkedins', date_since, current_datetime)

    print ('End of saving')

    if conn:
        conn.close()
    if mail:
        mail.close()
        mail.logout()

def load_config(file_path='ymails.ini'):
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error JSON decoding {file_path}.")
        return None        

get_ymails()