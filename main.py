from cmath import e
import os
from datetime import datetime,timedelta,date
import urllib.request 
import boto3
import botocore

import smtplib

import time

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from lxml import etree
import math
import logging
from logging.config import fileConfig



bucket=os.getenv('S3_BUCKET_NAME', default='')
data_folder=os.getenv('DATA_FOLDER', default='')
sender=os.getenv('SENDER', default='')
recipients=os.getenv('MAIL_RECIPIENTS', default='')
data_end_point=os.getenv('DEFAULT_DATA_PAGE_END_POINT', default="https://s3-ap-northeast-1.amazonaws.com/")

# SMTP Config
EMAIL_HOST = os.getenv('EMAIL_HOST', default=None)
EMAIL_HOST_USER = os.getenv('EMAIL_HOST_USER', default=None)
EMAIL_HOST_PASSWORD = os.getenv('EMAIL_HOST_PASSWORD', default=None)
EMAIL_PORT = int(os.getenv('EMAIL_PORT', default=587))

statistic_file=os.getenv('STATISTICS_FILE', default='statistics.csv')
stock_info_title="date,open,high,low,close,RSV,K"
k_value_upperbound=float(os.getenv('K_VALUE_UPPERBOUND', default=80))
k_value_lowerbound=float(os.getenv('K_VALUE_LOWERBOUND', default=20))
yahoo_finance_ul=os.getenv('YAHOO_FINANCE_URL', default='https://finance.yahoo.com/quote/ETH-USD/history?period1={}&period2={}&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true')

fileConfig('logging_config.ini')
logger = logging.getLogger(__name__)
def caculate_rsv_k(arr):
    matrix_width = 6;
    matrix_height=len(arr)-1 # we have title, so the number of records will be length -1
    matrix= [[0 for x in range(matrix_width)] for y in range(matrix_height)]
    #source(tmp_arr): date,open,high,low,close,RSV,K
    #                   0   1    2    3    4    5  6
    indx=0
    idx=0
    for d in arr:
       # logger.info(d)
        if idx>0:
            tmp_str_arr=d.split(",")
            matrix[indx][0]=float(tmp_str_arr[4]) # close
            matrix[indx][1]=float(tmp_str_arr[2]) # high
            matrix[indx][2]=float(tmp_str_arr[3]) # low
            matrix[indx][3]=float(tmp_str_arr[1]) # open
            if len(tmp_str_arr)>=6: # rsv exists
                matrix[indx][4]=float(tmp_str_arr[5])
            else:
                matrix[indx][4]=0.0
            if len(tmp_str_arr)>=7 : #daily K value exists
                matrix[indx][5]=float(tmp_str_arr[6])
            else:
                matrix[indx][5]=0.0 # daily K value
           # logger.info(d+","+str(matrix[indx][4]))
            indx=indx+1
            
        idx=1
        
    sidx=0
    upperbound=9 # K value: 9
    idx=0
    for idx in range(matrix_height):
        min_value=50000
        max_value=-50000
        sidx=0
        
        if idx>=(upperbound-1) and (idx)<matrix_height and matrix[idx][4]==0.0:
            # get max value and min value in upperbound days
            for sidx in range(upperbound):
                if max_value < matrix[idx-sidx][1]:
                    max_value=matrix[idx-sidx][1]
                 
                if min_value > matrix[idx-sidx][2]:
                    min_value=matrix[idx-sidx][2]
           
            matrix[idx][4]=(matrix[idx][0]-min_value)/(max_value-min_value)
            #logger.info("Hey:")
            #logger.info(matrix[idx][4])
            
    for idx in range(matrix_height):
        if idx > 0 and matrix[idx][5]==0.0:
            if idx< (upperbound-1):
                matrix[idx][5]=0.0
            elif idx == (upperbound-1):
                matrix[idx][5]=50.0
            else:
                matrix[idx][5]=100.0/3*matrix[idx][4]+2/3.0*matrix[(idx-1)][5]

    result=[]
    idx=0
    for d in arr:
        #logger.info(d)
        tmp_arr=d.split(",")
        if idx > 0:
            result.append(tmp_arr[0]+','+tmp_arr[1]+','+tmp_arr[2]+','+tmp_arr[3]+','+tmp_arr[4]+','+str(round(matrix[(idx-1)][4],3))+','+str(round(matrix[(idx-1)][5],3)))
            #logger.info(d+','+str(round(matrix[(idx-1)][4],3))+','+str(round(matrix[(idx-1)][5],3)))
        else:
            result.append(d)
        
        idx=idx+1
  
    return result      
    
def update_stock_info_in_s3(input_data):
    s3=boto3.resource('s3')
    
    tmp_arr=[]
    yyyy_mm_dd_timestamp_set=set()
    
    try: 
        bucketObj=s3.Bucket(bucket)
        bucketObj.download_file(data_folder+'/'+statistic_file,'/tmp/'+statistic_file)
        single_stock_symbol_file = open('/tmp/'+statistic_file, 'r')
        for l in single_stock_symbol_file:
            tmp_arr.append(l.strip())
            yyyy_mm_dd_timestamp_set.add(l.split(',')[0].strip())
        single_stock_symbol_file.close()
        
        #logger.info(input_data)
        if input_data != None and len(input_data) > 0:
            for d in input_data:

                yyyy_mm_dd_timestamp=(d[0])
            
                if yyyy_mm_dd_timestamp not in yyyy_mm_dd_timestamp_set:
                    tmp_arr.append(yyyy_mm_dd_timestamp+","+d[1].replace(',','')+","+d[2].replace(',','')+","+d[3].replace(',','')+","+d[4].replace(',',''))
        #logger.info(tmp_arr)        
        tmp_arr_rsv_k=caculate_rsv_k(tmp_arr)
        #logger.info(tmp_arr_rsv_k)
        # prepate output data 
        last_k_value_and_date=None
        output_data= ''
        for d in tmp_arr_rsv_k:
            output_data=output_data+d+'\n'
        last_k_value_and_date=tmp_arr_rsv_k[len(tmp_arr_rsv_k)-1].split(',')[6]+','+tmp_arr_rsv_k[len(tmp_arr_rsv_k)-1].split(',')[0]
        #logger.info(last_k_value_and_date)
        s3client=boto3.client('s3')
        # remove file on s3 
        response=s3client.delete_object(Bucket=bucket, Key=data_folder+'/'+statistic_file)

        # upload+set public
        s3client.put_object(Bucket=bucket, ContentType='text/plain', 
                            Key=data_folder+'/'+statistic_file,Body=output_data, ACL='public-read')
   
        return last_k_value_and_date
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object ("+data_folder+'/'+statistic_file+") does not exist.")
            return None
        else:
            raise
    
def notify_by_mail(mail_subject, mail_body, priority=None):
    current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = MIMEText(mail_body+'\n* This report is generated at '+current_time)
    msg['Subject'] = mail_subject
    msg['From'] = sender
    msg['To'] = recipients
    if priority != None and int(priority) >=1 and int(priority)<=5:
        msg['X-Priority'] = str(priority)
   
    s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.send_message(msg)
    s.quit()


def standardize_date(input_date):
    monthy_dic={
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    yyyy=input_date[-4:]
    mm=monthy_dic[input_date[:3]]
    dd=input_date[4:6]
    return yyyy+"-"+mm+"-"+dd

def getValurFromXpath(html, xpath):
    v=html.xpath(xpath)
    if v == None or len(v)==0:
        return ''
    else:
        return v[0]

def get_statistics():
    result=[]
    tr_index=1 
    upperbound=7 # I don't know how to know when we get the last </tr> in <tbody>
    current_unixtimestamp=time.time()
    current_unixtimestamp=math.floor(current_unixtimestamp)
    start_unixtimestamp=current_unixtimestamp - 86400 *upperbound
    source_url=yahoo_finance_ul.format(start_unixtimestamp, current_unixtimestamp)
    logger.info('Download ethereum statistics from: {}'.format(source_url))
    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'
    headers={'User-Agent': user_agent}
    req=urllib.request.Request(source_url, headers=headers)
    with urllib.request.urlopen(req) as f:
        content=f.read().decode('utf-8')
        html=etree.HTML(content)
        while tr_index <= upperbound:
            current_date=getValurFromXpath(html, 
                '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{}]/td[{}]/span/text()'.format(tr_index, 1))
            open_value=getValurFromXpath(html, 
                '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{}]/td[{}]/span/text()'.format(tr_index, 2))
            high_value=getValurFromXpath(html, 
                '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{}]/td[{}]/span/text()'.format(tr_index, 3))
            low_value=getValurFromXpath(html, 
                '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{}]/td[{}]/span/text()'.format(tr_index, 4))
            close_value=getValurFromXpath(html, 
                '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{}]/td[{}]/span/text()'.format(tr_index, 5))
            adj_close=getValurFromXpath(html, 
                '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{}]/td[{}]/span/text()'.format(tr_index, 6))
            volume=getValurFromXpath(html, 
                '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[2]/table/tbody/tr[{}]/td[{}]/span/text()'.format(tr_index, 7))
            if open_value != '':
                e=[]
                e.append(standardize_date(current_date))
                e.append(open_value)
                e.append(high_value)
                e.append(low_value)
                e.append(close_value)
                result.append(e)
            tr_index=tr_index+1
    result.sort()
    return result

def lambda_handler(event, context):
    
    # step 1: get the latest ethereum info
    logger.info('Step 1: download ethereum info from Yahoo Finance')
    today_update=get_statistics()
    logger.info(today_update)
    logger.info('')

    # step 2: update data and save in s3
    logger.info('Step 2: update data and save in s3')
    result_k_value=update_stock_info_in_s3(today_update)

    logger.info('the latest K value(K value/Date):')
    logger.info(result_k_value)
    logger.info('')
    
    # step 3:
    # email out if 1) K value <=20
    #              2) K value >=80
    logger.info('Step 3: generate the report and email')
    k_value=float(result_k_value.split(',')[0])
    data_update_time=result_k_value.split(',')[1]
    url_str=data_end_point+'/'+statistic_file+'\n'
 #   
    report_date_update_time=data_update_time.replace('-','/')
    action_str='今天 Ethereum K 值分析 ('+report_date_update_time+'): \n'

    if k_value > k_value_lowerbound and k_value < k_value_upperbound:
        action_str=action_str+"    不用買或賣 Ethereum (K 值={})\n".format(k_value)
    if k_value <= k_value_lowerbound:
        action_str=action_str+"    建議購買 Ethereum (K 值={})\n".format(k_value)
    if k_value >= k_value_upperbound:
        action_str=action_str+"    建議賣掉 Ethereum (K 值={})\n".format(k_value)
    
    action_str=action_str+'\n\n'+'詳細 K 值資訊 (Ethereum): \n'+url_str
    logger.info("email content: \n{}".format(action_str))
    
    if k_value <= k_value_lowerbound or k_value >= k_value_upperbound:
        notify_by_mail("[注意!!][K值分析] "+report_date_update_time+" Ethereum K 值="+str(k_value), action_str,1)
    else:
        notify_by_mail("[K值分析] "+report_date_update_time+" Ethereum K 值="+str(k_value),action_str)
 
if __name__ == "__main__":
    lambda_handler(None, None)
