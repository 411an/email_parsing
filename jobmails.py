import sqlite3
from bs4 import BeautifulSoup
from email import policy
from email.parser import BytesParser
import re
from string import punctuation

def preprocess_text(msgtext):
    text = msgtext.lower() 
    clean_text = re.sub(r"https?://\S+", "", text)    
    new_text = re.sub(f"[{re.escape(punctuation)}]", "", clean_text)
    new_text = re.sub(r"\n", " ", new_text)
    new_text = " ".join(new_text.split()) 
    pure_text = " ".join([w for w in new_text.split() if w.isalpha() or w.isspace()])
    return pure_text

def extract_text_from_email(email_body):
    parser = BytesParser(policy=policy.default)
    email_message = parser.parsebytes(email_body)
    text_parts = email_message.get_body(preferencelist=('plain'))
    html_parts = email_message.get_body(preferencelist=('html'))
    if html_parts:
        html_content = html_parts.get_content()
        soup = BeautifulSoup(html_content, 'html.parser')
        text_message = soup.get_text()
    elif text_parts:
        text_message = text_parts.get_content()
    else:
        text_message = ''
    return text_message

def first_messages_checking():
    conn = sqlite3.connect('ymails.db')
    read_cursor = conn.cursor()  
    write_cursor = conn.cursor()  

    read_cursor.execute("SELECT * FROM mainmails")
    
    x = 0
    changes = []  
    
    for email in read_cursor:
        sender = email[1]
        subject = email[2]
        date = email[5]
        body = email[3]
        messageid = email[4]

        big_msg = extract_text_from_email(body)
        clean_msg = preprocess_text(big_msg)
        
        changes.append((messageid, date, sender, subject, clean_msg))

        write_cursor.execute("INSERT INTO jobmails (messageid, date, sender, subject, msg) VALUES (?, ?, ?, ?, ?)",
                        (messageid, date, sender, subject, clean_msg))
        conn.commit()

        x = x + 1
        if x % 100 == 0:
            print(f"{x} emails")

    write_cursor.executemany("INSERT INTO jobmails (messageid, date, sender, subject, msg) VALUES (?, ?, ?, ?, ?)", changes)
    conn.commit()
    conn.close()

first_messages_checking()
