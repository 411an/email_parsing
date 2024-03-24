import os
os.environ['TRANSFORMERS_CACHE'] = 'E:/models/'
from transformers import pipeline
import sqlite3
import pandas as pd
from collections import Counter

def text_messages_checking():
    conn = sqlite3.connect('ymails.db')
    main_df = pd.read_sql_query("SELECT * FROM jobmails", conn)
    conn.close()
    texts = main_df[['messageid','msg']].values.tolist()
    return texts

def check_language(text):
    if any(char in "åäö" for char in text.lower()):
        return "sw"
    elif any(char in "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" for char in text.lower()):
        return "ru"
    elif not any(char in "abcdefghijklmnopqrstuvwxyz" for char in text.lower()):
        return "other"
    else:
        return "en"

def short_meaning(big_df):

    replacement_dict = {
        'This text about the job application has been received and it will be reviewed soon': 'apply',
        'This text about the job application already was reviewed and the company unfortunately move on with other candidates': 'cancel',
        'This text contains subscriptions, newsletters or private messages about personal things': 'other',
        'Denna text innehåller prenumerationer nyhetsbrev eller privata meddelanden om personliga saker': 'other',
        'Denna text om företag som har gått din ansökan och kommer att se över det snart':'apply',
        'Denna text om jobbansökan och företaget går tyvärr vidare med andra kandidater':'cancel'
    }

    big_df = big_df.rename(columns=replacement_dict)
    return big_df

def process_message(msg, classifier_set,language):
    m_id, text = msg
    hypotheses = classifier_set["hypotheses"]
    results_df = pd.DataFrame(columns=["messageid"] + hypotheses + ["model"])

    for model_name, classifier in classifier_set.items():
        if model_name in ["hypotheses"]:
            continue
        
        if language == 'sw':
            results = classifier(text[:512], hypotheses)
        else:
            results = classifier(text, hypotheses)            

        model_scores = {label: score for label, score in zip(results["labels"], results["scores"])}
        model_scores["messageid"] = m_id
        model_scores["model"] = model_name

        model_df = pd.DataFrame([model_scores])
        results_df = pd.concat([results_df, model_df], ignore_index=True)

    sum_scores = results_df.groupby("messageid")[hypotheses].sum()
    sum_scores["messageid"] = sum_scores.index
    sum_scores["model"] = "summa"
    results_df = pd.concat([results_df, sum_scores], ignore_index=True)

    results_df = short_meaning(results_df)

    return results_df

def models_working():
    conn = sqlite3.connect('ymails.db')
    cursor = conn.cursor()

    classifiers = {
        "en": {
            "bart": pipeline("zero-shot-classification", model="facebook/bart-large-mnli"),
            "roberta": pipeline("zero-shot-classification", model="roberta-large-mnli"),
            "deberta": pipeline("zero-shot-classification", model="MoritzLaurer/deberta-v3-large-zeroshot-v1.1-all-33"),
            "hypotheses": [
                "This text contains subscriptions, newsletters or private messages about personal things",
                "This text about the job application has been received and it will be reviewed soon",
                "This text about the job application already was reviewed and the company unfortunately move on with other candidates"
            ]
        },
        "sw": {
            "bart": pipeline("zero-shot-classification", model="KBLab/megatron-bert-large-swedish-cased-165-zero-shot"),
            "roberta": pipeline("zero-shot-classification", model="alexandrainst/scandi-nli-base"),
            "deberta": pipeline("zero-shot-classification", model="MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7"),
            "hypotheses": [
                "Denna text innehåller prenumerationer nyhetsbrev eller privata meddelanden om personliga saker",
                "Denna text om företag som har gått din ansökan och kommer att se över det snart",
                "Denna text om jobbansökan och företaget går tyvärr vidare med andra kandidater"
            ]
        }
    }


    text_strings = text_messages_checking()
    all_counter = 0

    for msg in text_strings:
        all_counter += 1
        if all_counter % 100 == 0:
            print(f"{all_counter} писем")

        message_id_exists = cursor.execute("SELECT EXISTS(SELECT 1 FROM all_mails WHERE messageid=?)", (msg[0],)).fetchone()[0]
        if message_id_exists:
            print(f"ID {msg[0]} is already exist.")
            continue

        language = check_language(msg[1][:512])
        if language not in ["en","sw"]:
            print("Not a good language.")
            continue

        classifier_set = classifiers[language]

        predf = process_message(msg, classifier_set,language)
        
        predf.to_sql('all_mails', conn, if_exists='append', index=False)
        conn.commit()        
        print("OK")
        

    conn.close()

models_working()