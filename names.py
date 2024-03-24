import os
os.environ['TRANSFORMERS_CACHE'] = 'E:/models/'
from transformers import pipeline
import sqlite3
import re
from string import punctuation
from bs4 import BeautifulSoup
from email import policy
from email.parser import BytesParser

def process_and_insert_data(messageid, category, date, msg, company, job_title, write_cursor, conn):
    print("\033[31m" + company + " | " + job_title + "\033[0m")

    write_cursor.execute("INSERT INTO mailinfo (messageid, category, date, msg, company, jobtitle) VALUES (?, ?, ?, ?, ?, ?)",
                   (messageid, category, date, msg, company, job_title))
    conn.commit()

def clean_text(text):
    сleaned_msg = text.replace('\u200c', '').strip()
    cleaned_text = ' '.join(сleaned_msg.split())
    return cleaned_text

def linkedin_company_and_position(msgtext):

    text = clean_text(msgtext)

    if "your application was sent to " in text:
        start_phrase = "your application was sent to "
        end_phrase = " your application was sent to"
        
        start_index = text.find(start_phrase) + len(start_phrase)
        end_index = text.find(end_phrase, start_index)
        company_name = text[start_index:end_index]

        position_start_phrase = company_name + " "
        position_end_phrase = " " + company_name
        position_start_index = text.find(position_start_phrase, end_index) + len(position_start_phrase)
        position_end_index = text.find(position_end_phrase, position_start_index)
        position = text[position_start_index:position_end_index]

        return company_name, position.strip()

    elif "your update from " in text:
        company_start_phrase = "update on your "
        company_end_phrase = " application your"
        
        company_start_index = text.find(company_start_phrase) + len(company_start_phrase)
        company_end_index = text.find(company_end_phrase, company_start_index)
        company_name = text[company_start_index:company_end_index]

        position_start_phrase = "your update from " + company_name + " "
        position_start_index = text.find(position_start_phrase, company_end_index) + len(position_start_phrase)
        position_end_index = text.find(" applied on", position_start_index)
        position = text[position_start_index:position_end_index]

        return company_name.strip(), position.strip()

    else:
        return None, None 

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

def check_language(text):
    if any(char in "åäö" for char in text.lower()):
        return "sw"
    elif not any(char in "abcdefghijklmnopqrstuvwxyz" for char in text.lower()):
        return "other"
    else:
        return "en"

def extract_company_title(text, qa_model):
    question = "What is the company name?"
    answer = qa_model(question=question, context=text)
    return answer["answer"]

def extract_job_title(text, language, qa_model_en, qa_model_se):
    if language == "en":
        question = "What is the job title in the text?"
        qa_model = qa_model_en
    elif language == "sw":
        question = "Vad är jobbtiteln i texten?"
        qa_model = qa_model_se
        text = text[:512]
    answer = qa_model(question=question, context=text)
    return answer["answer"]

conn = sqlite3.connect('ymails.db')
cursor = conn.cursor()
write_cursor = conn.cursor()

english_qa_model = pipeline("question-answering", model="bert-large-uncased-whole-word-masking-finetuned-squad")
english_qa_model1 = pipeline("question-answering", model="deepset/roberta-base-squad2")
english_qa_model2 = pipeline("question-answering", model="timpal0l/mdeberta-v3-base-squad2")
english_qa_model3 = pipeline("question-answering", model="allenai/longformer-large-4096-finetuned-triviaqa")

swedish_qa_model = pipeline("question-answering", model="monakth/bert-base-swedish-cased-sv2")

cursor.execute("""
    SELECT
        a.messageid,
        CASE 
            WHEN COALESCE(a.cancel, 0) > COALESCE(a.apply, 0) AND COALESCE(a.cancel, 0) > COALESCE(a.other, 0) THEN 'cancel'
            WHEN COALESCE(a.apply, 0) > COALESCE(a.cancel, 0) AND COALESCE(a.apply, 0) > COALESCE(a.other, 0) THEN 'apply'
            ELSE 'other'
        END AS category,
        CASE 
            WHEN COALESCE(a.cancel, 0) > COALESCE(a.apply, 0) AND COALESCE(a.cancel, 0) > COALESCE(a.other, 0) THEN COALESCE(a.cancel, 0)
            WHEN COALESCE(a.apply, 0) > COALESCE(a.cancel, 0) AND COALESCE(a.apply, 0) > COALESCE(a.other, 0) THEN COALESCE(a.apply, 0)
            ELSE COALESCE(a.other, 0)
        END AS cat_value,
        j.msg,
        j.date,
        j.sender,
        mm.body AS mbody
    FROM
        all_mails a
    LEFT JOIN
        jobmails j ON a.messageid = j.messageid
    LEFT JOIN               
        mainmails mm ON a.messageid = mm.messageid
    WHERE
        a.model = 'summa' AND category <> 'other'
""")

results = cursor.fetchall()

main_counter = 0

for row in results:
    main_counter = main_counter  + 1
    messageid = row[0]
    category = row[1]
    cat_value = row[2]
    msg = row[3]
    date = row[4]
    sender = row[5]
    mbody = row[6]

    if main_counter % 100 == 0:
        print(f"{main_counter} emails")
    #linkedin filter
    if any(keyword in msg for keyword in ['top job picks for you','your job alert for', 'jobs similar to', 'your job recommendations','any updates on your applications']):
        print("Linkedin, skip")
        continue

    cursor.execute("SELECT * FROM mailinfo WHERE messageid = ?", (messageid,))
    result = cursor.fetchone()
    if result:
        print("ID {} is already exist".format(messageid))
        continue    

    if "jobs-noreply@linkedin.com" in sender:
        company, job_title = linkedin_company_and_position(msg)
        if company is None and job_title is None:
            continue
        process_and_insert_data(messageid, category, date, msg, company, job_title, write_cursor, conn)
        continue    
    else:
        pass

    language = check_language(msg[:512])
    #preferrable jobs filter
    jobs_to_check = ['data', 'analy']
    models_to_try = [english_qa_model3, english_qa_model2, english_qa_model1, english_qa_model]

    #first cheking for job title
    for model in models_to_try:
        job_title = extract_job_title(msg, language, model, swedish_qa_model)
        if any(keyword in job_title for keyword in jobs_to_check) and job_title not in ['teamtailor','team']: #filters from HR system
            break

    #second cheking for job title
    if any(keyword in job_title for keyword in jobs_to_check) and job_title not in ['teamtailor','team']: #filters from HR system
        msg_for_job = extract_text_from_email(mbody)
        msg_for_job = preprocess_text(msg_for_job)
        for model in models_to_try:
            job_title = extract_job_title(msg_for_job, language, model, swedish_qa_model)
            if any(keyword in job_title for keyword in jobs_to_check) and job_title not in ['teamtailor','team']: #filters from HR system
                break
    
    company = extract_company_title(sender, english_qa_model)
    
    words_to_check = ['stockholm','.', '@', 'teamtailor','no-reply','noreply', 'LinkedIn', job_title]

    models_to_check = [english_qa_model1, english_qa_model2, english_qa_model3]

    found_symbols = False
    for index, model in enumerate(models_to_check):
        company = extract_company_title(sender, model)
        # print(company + " - sender from model '{}'".format(index))
        if all(company.find(symbol) == -1 for symbol in words_to_check):
            #print("Answer from model '{}'".format(index))
            found_symbols = True
            break

    if not found_symbols:
        for index, model in enumerate(models_to_check):
            company = extract_company_title(msg, model)
            #print(company + " - msg from model '{}'".format(index))            
            if all(company.find(symbol) == -1 for symbol in words_to_check):
                #print("Answer from model '{}'".format(index))
                found_symbols = True
                break

    if not found_symbols:
        msg_for_company = extract_text_from_email(mbody)
        msg_for_company = preprocess_text(msg_for_company)
        for index, model in enumerate(models_to_check):
            company = extract_company_title(msg_for_company, model)
            #print(company + " - msg-body from model '{}'".format(index))            
            if all(company.find(symbol) == -1 for symbol in words_to_check):
                #print("Answer from model '{}'".format(index))
                found_symbols = True
                break

    if not found_symbols:
        print("Not a good answer, check it manually")
    
    
    if ',' in company:
        company = company.split(',')[1].strip()

    process_and_insert_data(messageid, category, date, msg, company, job_title, write_cursor, conn)

    print("\033[31m" + company + " | " + job_title + "\033[0m")

    #Запись полученных данных в таблицу mailinfo
    write_cursor.execute("INSERT INTO mailinfo (messageid, category, date, msg, company, jobtitle) VALUES (?, ?, ?, ?, ?, ?)",
                   (messageid, category, date, msg, company, job_title))
    conn.commit()

conn.close()