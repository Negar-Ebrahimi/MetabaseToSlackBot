import json
import requests
import env
import numpy as np
import pandas as pd
from datetime import datetime

class Data:
    METABASE_SESSION = ""

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

def transformPivotResponseToDataFrame(pivotResponse):
    job_audit_df = pd.DataFrame(
        pivotResponse.get("rows"),
        columns = pivotResponse.get("columns")
    )[
        [
            "id",
            "OrderId",
            "PlaceCategory",
            "WorkflowStepId",
            "previous_workflowstepid",
            "next_workflowstepid",
            "nth",
            "CreatedDate",
            "UpdatedDate",
            "UpdatedBy"
        ]
    ]
    return job_audit_df

def extractPrebookOrders(job_audit_df):
    prebook_orders_list = []
    prebook_flag = 0
    for indx, job_audit in job_audit_df.iterrows():
        
        if job_audit_df["nth"].iloc[indx] == 1:
            if prebook_flag == 1:
                prebook_flag = 0
                prebook_orders_list.append(prebook_list)
            if job_audit_df["WorkflowStepId"].iloc[indx] == "init" \
            and job_audit_df["next_workflowstepid"].iloc[indx] == "init":
                prebook_flag = 1
                prebook_list = []
        if prebook_flag == 1:
            prebook_list.append(job_audit)
    return prebook_orders_list
    
def extractInstantOrders(job_audit_df):
    instant_orders_list = []
    instant_flag = 0
    for indx, job_audit in job_audit_df.iterrows():     
        if job_audit_df["nth"].iloc[indx] == 1:
            if instant_flag == 1:
                instant_flag = 0
                instant_orders_list.append(instant_list)
            if job_audit_df["WorkflowStepId"].iloc[indx] == "init" \
            and job_audit_df["next_workflowstepid"].iloc[indx] != "init":
                instant_flag = 1
                instant_list = []
        if instant_flag == 1:
            instant_list.append(job_audit)
    return instant_orders_list
    
def splitOrderPathToSubPaths(prebook_batches):
    prebook_splitted_batches = []
    for prebook_batch in prebook_batches:
            being_splitted_list = []
            counting_off_flag = 0
            for job_audit in prebook_batch:
                if counting_off_flag == 0:
                    being_splitted_list.append(job_audit)
                if job_audit["WorkflowStepId"] == "payment" \
                or job_audit["WorkflowStepId"] == "supplydecline":
                    counting_off_flag = 1
                if (job_audit["WorkflowStepId"] == "paymenttimeout" \
                    and job_audit["next_workflowstepid"] == "init"):
                    prebook_splitted_batches.append(being_splitted_list)
                    being_splitted_list = []
                    counting_off_flag = 0
            prebook_splitted_batches.append(being_splitted_list)
                
    return prebook_splitted_batches

def inferTheStepUserTypeFromJobAudit(job_audit):
    StepUserType = "Jabama"
    if not (job_audit["WorkflowStepId"] == "payment" \
    or job_audit["WorkflowStepId"] == "supplydecline"):
        return StepUserType
    if (("jabama" not in job_audit["UpdatedBy"][1:].lower()
    or job_audit["UpdatedBy"][0:6].lower() == "jabama"
    or job_audit["UpdatedBy"][0] == "0" \
    or job_audit["UpdatedBy"][0] == "+") \
    and not(job_audit["UpdatedBy"][-10:].lower() == "alibaba.ir")):
        StepUserType = "User"
    return StepUserType
  
def calculateTimeToHostConclusion_df(prebook_splitted_batches):
    host_conclusion_prebook_splitted_batches = pd.DataFrame(dtype=float, columns = ["sub_order", "THC", "HostConcludingUserType", "HostConclusionType"])
    for prebook_splitted_batch in prebook_splitted_batches:
        start_dt = prebook_splitted_batch[0]["CreatedDate"]
        if start_dt[22] == "+":
            start_dt = start_dt[:22] + '0' + start_dt[22:]
        if start_dt[21] == "+":
            start_dt = start_dt[:21] + '00' + start_dt[21:]
        if start_dt[19] == "+":
            start_dt = start_dt[:19] + '.000' + start_dt[19:]
            
        end_dt = prebook_splitted_batch[-1]["UpdatedDate"]
        if end_dt[22] == "+":
            end_dt = end_dt[:22] + '0' + end_dt[22:]
        if end_dt[21] == "+":
            end_dt = end_dt[:21] + '00' + end_dt[21:]
        if end_dt[19] == "+":
            end_dt = end_dt[:19] + '.000' + end_dt[19:]
  
        THC = datetime.fromisoformat(end_dt) - datetime.fromisoformat(start_dt)
        ConcludingUserType = inferTheStepUserTypeFromJobAudit(prebook_splitted_batch[-1])
        
        ConclusionType = "Decline"
        if prebook_splitted_batch[-1]["WorkflowStepId"] == "payment":
            ConclusionType = "Payment"
            
        host_conclusion_prebook_splitted_batches = host_conclusion_prebook_splitted_batches.append({
         "sub_order": prebook_splitted_batch,
         "THC": (THC.seconds//60)%60,
         "HostConcludingUserType": ConcludingUserType,
         "HostConclusionType": ConclusionType
        }, ignore_index=True)
    return host_conclusion_prebook_splitted_batches

def host_conclusion_job():
    #Get and store the raw data of union of questions from Metabase.
    pivot_metacard_responses = getMetaCards([env.CARD_ID_JOBAUDIT])
    pivot_responses_data = getDataFromPivotResponses(pivot_metacard_responses)
    job_audit_df = transformPivotResponseToDataFrame(pivot_responses_data[0])
    
    prebook_order_batches_list = extractPrebookOrders(job_audit_df)
    instant_order_batches_list = extractInstantOrders(job_audit_df)

    prebook_splitted_batches = splitOrderPathToSubPaths(prebook_order_batches_list)
    prebook_splitted_batches_with_THC = calculateTimeToHostConclusion_df(prebook_splitted_batches)

    total_prebook_splitted_batches = len(prebook_splitted_batches_with_THC)
    THC_aggregated_df = prebook_splitted_batches_with_THC.groupby(["HostConcludingUserType", "HostConclusionType"], as_index = False).agg([np.median, 'count'])
    THC_aggregated_df.columns = THC_aggregated_df.columns.droplevel(0)
    THC_aggregated_df = THC_aggregated_df.reset_index()[["HostConcludingUserType", "HostConclusionType", "median", "count"]]
    THC_aggregated_df = THC_aggregated_df.rename(columns={"median": "THC_median", "count": "SubOrders_Count"})
    THC_aggregated_df["Percentage"] = THC_aggregated_df["SubOrders_Count"] / total_prebook_splitted_batches * 100
    THC_aggregated_df["Percentage"] = THC_aggregated_df["Percentage"].map("{:,.2f}%".format)
    
    THC_result_df = THC_aggregated_df[["HostConcludingUserType", "HostConclusionType", "Percentage", "THC_median"]]
    return THC_result_df
