import json
import requests
import copy
import schedule
import time

# Metabot's own modules
import env
import THC_calculation as thc

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
        url = "http://metabase.jabama.com/api/card/" + str(card_id) + "/query"

        headers = {
            'Content-Type': "application/json",
            'X-Metabase-Session': Data.METABASE_SESSION,
            }

        responses.append([card_id, requests.request("POST", url, headers=headers)])
    return responses

def getDataFromTableResponses(responses):
    responses_data = []
    for response in responses:
        rows = json.loads(response[1].text)['data']['rows']
        card_id = response[0]
        responses_data.append({
            "card_id": card_id,
            "rows": rows
        })
        
    return responses_data

def getDataFromPivotResponses(responses):
    responses_data = []
    
    for response in responses:
        cols = json.loads(response[1].text)['data']['cols']
        rows = json.loads(response[1].text)['data']['rows']
        card_id = response[0]
        col_titles = []
        for col in cols:
            col_titles.append(col['name'])

        responses_data.append({
            "card_id": card_id,
            "columns": col_titles,
            "rows": rows
        })
    return responses_data

def formatMessageBlocks(pivot_responses = [], table_responses = []):
    message_blocks = []
    for pivot_response in pivot_responses:
        plain_text = ""
        columns = copy.deepcopy(pivot_response).get("columns")
        if columns == []:
            columns = ['']
        columns.pop(0)
        for row in pivot_response.get("rows"):
            plain_text = plain_text + "*" + str(row[0]) + "*\n"
            indx = 1
            for col in columns:
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

def getUserIDByEmail(email):
    querystring = {
        "email" : email
    }
    headers = {
        'Content-Type': "application/json",
        'Authorization': "Bearer " + env.METABOT_TOKEN,
        }
    response = requests.request("GET", env.SLACK_API_LOOK_UP_BY_EMAIL, headers=headers, params=querystring)
    if json.loads(response.text)['ok']:
        response = json.loads(response.text)['user']['id']
    return response
        
def sendMessagesToSlackUser(user_id, message_blocks):
    responses = []
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
        'status': json.loads(response.text)['ok']
    })
    return responses

def TransformDataframeToTableReponse(df):
    list_of_rows = []
    for index, row in df.iterrows():
        list_of_rows.append(row)
    table_response = {'card_id': 0, 'rows': list_of_rows}
    return table_response

def metabot_job():
    #Get the recipients' list from a MetaCard, extract the list of their email addresses and
    #their personalized question cardIDs from it.
    recipients_metacard = getMetaCards([
        env.CARD_ID_METABOT_RECIPIENTS
    ])   
    send_what_to_whom = [
        [
            recipient[0],
            recipient[1],
            [] if recipient[2] == '' else [int(pivot_question_cardIDs) for pivot_question_cardIDs in recipient[2].split(',')],
            [] if recipient[3] == '' else [int(table_question_cardIDs) for table_question_cardIDs in recipient[3].split(',')],
            recipient[4]
        ] for recipient in (getDataFromTableResponses(recipients_metacard)[0]['rows'])]
    #End of getting the recipients' list
    
    #Get the union list of pivot question cardIDs and table question cardIDs.
    unified_pivot_cardIDs = list(set().union(*[send_what_to_a_recipient[2] for send_what_to_a_recipient in send_what_to_whom]))
    unified_table_cardIDs = list(set().union(*[send_what_to_a_recipient[3] for send_what_to_a_recipient in send_what_to_whom]))
    #End of getting the union of questions' cardIDs.
    
    #Get and store the raw data of union of questions from Metabase.
    pivot_metacard_responses = getMetaCards(unified_pivot_cardIDs)
    pivot_responses_data = getDataFromPivotResponses(pivot_metacard_responses)
    table_metacard_responses = getMetaCards(unified_table_cardIDs)
    table_responses_data = getDataFromTableResponses(table_metacard_responses)
    #End of creating the message blocks
    
    #Get the recipients' Slack ID by their emails and send their personalized message blocks to them.
    for send_what_to_a_recipient in send_what_to_whom:
        user_id = getUserIDByEmail(send_what_to_a_recipient[1])
        pivot_question_cardIDs = send_what_to_a_recipient[2]
        table_question_cardIDs = send_what_to_a_recipient[3]
        receive_THC = send_what_to_a_recipient[4]
        if user_id == [] or (pivot_question_cardIDs == [] and table_question_cardIDs == []):
            return
        message_blocks= []
        #Extract the user's personalized question responses to be included in their message block
        this_user_pivot_responses_data = []
        for pivot_question_cardID in pivot_question_cardIDs:
            for pivot_response_data in pivot_responses_data:
                if pivot_response_data['card_id'] == pivot_question_cardID:
                    this_user_pivot_responses_data.append(pivot_response_data)                   
        this_user_table_responses_data = []
        for table_question_cardID in table_question_cardIDs:
            for table_response_data in table_responses_data:
                if table_response_data['card_id'] == table_question_cardID:
                    this_user_table_responses_data.append(table_response_data)
        #End of extracting this user's personalized question responses 
        
        #Include the THC report in this recipient's responses data if his name is among those who should receive it.
        if receive_THC == True:
            thc.Data.METABASE_SESSION = Data.METABASE_SESSION
            thc_result_df = thc.host_conclusion_job()
            thc_table_response = TransformDataframeToTableReponse(thc_result_df)
            this_user_table_responses_data.append(thc_table_response)
        #End of including the THC report in this recipient's responses data

        email = send_what_to_a_recipient[1]
        user_id = getUserIDByEmail(email)

        message_blocks = formatMessageBlocks(
            this_user_pivot_responses_data,
            this_user_table_responses_data
        )
   
        send_messages_status = sendMessagesToSlackUser(user_id, message_blocks)
    #End of sending the message blocks to recipients
    
    
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