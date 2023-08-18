import os
from marketorestpython.client import MarketoClient
import datetime
from time import sleep


# Editable Key variables
munchkin_id = '' #input requried
client_id = '' #input requried
client_secret = '' #input requried

#Set working Directories for the Script
base_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(base_dir)

# Authenticate with Marketo
mc = MarketoClient(munchkin_id, client_id, client_secret)

def Activity_Jobs(f_st_date,f_ed_date):
    try:
        
        #Define Job
        All_lead_Activities_job=  mc.execute(method='create_activities_export_job', 
                        filters={'createdAt': {'endAt': str(f_ed_date), 'startAt': str(f_st_date)}})

        #capture export job id
        export_id = All_lead_Activities_job[0]['exportId']

        #enqueue export job
        enqued_job_details = mc.execute(method='enqueue_activities_export_job',job_id=export_id)
        print(f"\nJob queued: {datetime.datetime.now()}\t\texport_id {export_id}")
        sleep(20)

        #poll job status until it completes
        export_job_status=mc.execute(method='get_activities_export_job_status',job_id=export_id)[0]["status"]
        while export_job_status not in ["Canceled","Completed","Failed"]:
            sleep(60)
            results  = mc.execute(method='get_activities_export_job_status',job_id=export_id)
            export_job_status = results[0]['status']
            print(f"\t{datetime.datetime.now()}: {export_job_status}")

        #if Completed successfully, save results to csv
        if export_job_status == "Completed":
            num_records = results[0]["numberOfRecords"]
            print(f"\t\t# of records: {num_records}")
            export_file_contents = mc.execute(method='get_activities_export_job_file',job_id=export_id)
            filename = "Bulk_Extract_Activities_"+str(f_st_date)+"_to_"+str(f_ed_date)+".csv"
            with open(filename, 'wb') as f:
                f.write(export_file_contents)
        else:
            print(f"\n\n*** \n Warning:job did not complete: {export_job_status}\n")
            raise ValueError('Marketo export failed to complete')
        
    except Exception as e:
        raise e

def Get_All_activities_Bulk(Start_Date,End_Date):
    loop_st_date = Start_Date #YYYY-MM-DD
    loop_ed_date = End_Date  #YYYY-MM-DD
    date_format = '%Y-%m-%d'
    loop_st_date = datetime.datetime.strptime(loop_st_date,date_format)
    loop_ed_date = datetime.datetime.strptime(loop_ed_date,date_format)
    Diff = loop_ed_date.date() - loop_st_date.date()
    temp_st_date = loop_st_date
    days_to_end = Diff

    while days_to_end.days > 0:
        if days_to_end.days < 29:
            print(f"Batch : Start date: {str(temp_st_date.date())} && End date:{str(loop_ed_date.date())}")
            Activity_Jobs(str(temp_st_date.date()),str(loop_ed_date.date()))
            break
        else:
            temp_dff_days = datetime.timedelta(days=29)
            temp_end_date = temp_st_date + temp_dff_days
            days_to_end = loop_ed_date - temp_end_date
            print(f"Batch : Start date: {str(temp_st_date.date())} && End date:{str(temp_end_date.date())}")
            Activity_Jobs(str(temp_st_date.date()),str(temp_end_date.date()))
            temp_st_date = temp_end_date

Date_start = input("Input Start Date for the Bulk Activities Export in #YYYY-MM-DD Format:")
Date_end = input("Input End Date for the Bulk Activities Export in #YYYY-MM-DD Format:")
Get_All_activities_Bulk(str(Date_start),str(Date_end))
