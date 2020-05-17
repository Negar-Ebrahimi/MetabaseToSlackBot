import json
import urllib.request

import codecs
import schedule
import time

import requests
import os
import env

class Data:
    METABASE_SESSION = ""
def getMetabaseSession(username, password):
    payload = {
        "username": username,
        "password": password
    }
    headers = {
        'content-type': "application/json",
    }
    response = requests.request("POST", env.METABASE_API_GET_SESSION, data=json.dumps(payload, separators=(',', ':')), headers=headers)
    Data.METABASE_SESSION = json.loads(response.text)['id']

def getMetaCards(card_ids):
    responses = []
    for card_id in card_ids:
        url = "http://metabase.jabama.com/api/card/" + card_id + "/query"

        headers = {
            'Content-Type': "application/json",
            'X-Metabase-Session': Data.METABASE_SESSION,
            }

        responses.append(requests.request("POST", url, headers=headers))
    return responses

def getDataFromTableResponses(responses):
    responses_data = []
    for response in responses:
        rows = json.loads(response.text)['data']['rows']
        responses_data.append({
            "rows": rows
        })
        
    return responses_data

def getDataFromPivotResponses(responses):
    responses_data = []
    
    for response in responses:
        cols = json.loads(response.text)['data']['cols']
        rows = json.loads(response.text)['data']['rows']
        col_titles = []
        for col in cols:
            col_titles.append(col['name'])

        responses_data.append({
            "columns": col_titles,
            "rows": rows
        })
    return responses_data

def formatMessageBlocks(pivot_responses, table_responses = []):
    message_blocks = []
    for pivot_response in pivot_responses:
        plain_text = ""
        pivot_response.get("columns").pop(0)
        for row in pivot_response.get("rows"):
            plain_text = plain_text + "*" + str(row[0]) + "*\n"
            indx = 1
            for col in pivot_response.get("columns"):
                plain_text = plain_text + col + ": " + str(row[indx]) + "\n"
                indx = indx + 1
            plain_text = plain_text + "\n"
        message_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": plain_text
            }
        })
        message_blocks.append({
            "type": "divider"
        })
        

    for table_response in table_responses:
        plain_text = ""
        for row in table_response.get("rows"):
            plain_text = plain_text + "*" + str(row[0]) + "*: "
            for indx in range(len(row) - 1):
                plain_text = plain_text + str(row[1+indx]) + "  "
            plain_text = plain_text + "\n"
        message_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": plain_text
            }
        })
        message_blocks.append({
            "type": "divider"
        })
    return message_blocks

def getUserIDByEmail(emails):
    responses = []
    for email in emails:  
        querystring = {
            "email" : email
        }
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer " + env.METABOT_TOKEN,
            }
        response = requests.request("GET", env.SLACK_API_LOOK_UP_BY_EMAIL, headers=headers, params=querystring)
        if json.loads(response.text)['ok']:
            responses.append(json.loads(response.text)['user']['id'])
    return responses
        
def sendMessagesToSlackUsers(user_ids, message_blocks):
    responses = []
    for user_id in user_ids:
        msg_indx = 0
        payload = {
        "channel" : user_id,
        "blocks": message_blocks,
        "as_user": True
        }
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer " + env.METABOT_TOKEN,
            }
        response = requests.request("POST", env.SLACK_API_POST_MESSAGE, data=str(payload), headers=headers)
        responses.append({
            'user_id': user_id,
            'message_index': msg_indx,
            'status': json.loads(response.text)['ok']
        })
        msg_indx = msg_indx + 1
    return responses

def metabot_job():
    pivot_metacard_responses = getMetaCards([
        env.CARD_ID_ACCOMMODATION,
        env.CARD_ID_ACCOMMODATION_BY_VENUE,
        env.CARD_ID_TOTAL,
        env.CARD_ID_SEARCH_CR
    ])
    pivot_responses_data = getDataFromPivotResponses(pivot_metacard_responses)
    
    table_metacard_responses = getMetaCards([
        env.CARD_ID_ORDER_STATUS
    ])
    table_responses_data = getDataFromTableResponses(table_metacard_responses)
    
    message_blocks = formatMessageBlocks(pivot_responses_data, table_responses_data)
    
    user_ids = getUserIDByEmail([
        env.EMAIL_HOSSEIN,
        env.EMAIL_AHMAD,
        env.EMAIL_NIMA_EBRAHIMI,
        env.EMAIL_MASOUD,
        env.EMAIL_RASHA,
        env.EMAIL_KAMYAB
    ])
    send_messages_status = sendMessagesToSlackUsers(user_ids, message_blocks)
    
def metabase_get_session_job():
    getMetabaseSession(env.METABASE_USERNAME, env.METABASE_PASSWORD)

schedule.every().day.at("08:59").do(metabot_job)
schedule.every().day.at("09:59").do(metabot_job)
schedule.every().day.at("10:59").do(metabot_job)
schedule.every().day.at("11:59").do(metabot_job)
schedule.every().day.at("12:59").do(metabot_job)
schedule.every().day.at("13:59").do(metabot_job)
schedule.every().day.at("14:59").do(metabot_job)
schedule.every().day.at("15:59").do(metabot_job)
schedule.every().day.at("16:59").do(metabot_job)
schedule.every().day.at("17:59").do(metabot_job)
schedule.every().day.at("18:59").do(metabot_job)
schedule.every().day.at("19:59").do(metabot_job)
schedule.every().day.at("20:59").do(metabot_job)
schedule.every().day.at("21:59").do(metabot_job)
schedule.every().day.at("22:59").do(metabot_job)
schedule.every().day.at("23:59").do(metabot_job)
schedule.every().monday.do(metabase_get_session_job)

metabase_get_session_job()

while 1:
    schedule.run_pending()
    time.sleep(1)

