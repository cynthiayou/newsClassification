from kafka import KafkaProducer
from kafka.errors import KafkaError
from time import sleep
import json, sys
import requests
import time
import pandas as pd
import numpy as np
from os import makedirs
from os.path import join, exists
from datetime import date, timedelta

def getData(fromDate, toDate, key):

    labels = pd.read_csv("hw3_label.csv", header=0, index_col=0)
    print("labels:\n ", labels)
    print("Politics label : ", labels.ix['Politics','0'])

    API_ENDPOINT = 'http://content.guardianapis.com/search'
    my_params = {
        'from-date': "",
        'to-date': "",
        'order-by': "newest",
        'show-fields': 'all',
        'page-size': 200,
        'api-key': key
    }

    res_data = []

    my_params['from-date'] = fromDate
    my_params['to-date'] = toDate
    current_page = 1
    total_pages = 1
    while current_page <= total_pages:
        print("...page", current_page)
        my_params['page'] = current_page
        resp = requests.get(API_ENDPOINT, my_params)
        jsonData = resp.json()

        # if there is more than one page
        current_page += 1
        total_pages = jsonData['response']['pages']

        for i in range(len(jsonData["response"]['results'])):
            headline = jsonData["response"]['results'][i]['fields']['headline'].replace('\n',' ').replace('\r',' ')
            bodyText = jsonData["response"]['results'][i]['fields']['bodyText'].replace('\n',' ').replace('\r',' ')
            headline += bodyText
            category = jsonData["response"]['results'][i]['sectionName']
            if category in labels.index:
                label = labels.ix[category,'0']
            else:
                label = str(sys.maxsize)
            #print("categorylabel and label: ", category + " " + str(label))
                # data.append({'label':labels[label],'Descript':headline})
            toAdd = str(label) + '||' + category + '||' + headline
            res_data.append(toAdd)

    return res_data

def getDataforLabel(fromDate, toDate, key):
    API_ENDPOINT = 'http://content.guardianapis.com/search'
    my_params = {
        'from-date': "",
        'to-date': "",
        'order-by': "newest",
        'show-fields': 'all',
        'page-size': 200,
        'api-key': key
    }

    res_data = []
    labels = {}
    index = 0

    my_params['from-date'] = fromDate
    my_params['to-date'] = toDate
    current_page = 1
    total_pages = 1
    while current_page <= total_pages:
        print("...page", current_page)
        my_params['page'] = current_page
        resp = requests.get(API_ENDPOINT, my_params)
        jsonData = resp.json()

        # if there is more than one page
        current_page += 1
        total_pages = jsonData['response']['pages']

        for i in range(len(jsonData["response"]['results'])):
            headline = jsonData["response"]['results'][i]['fields']['headline'].replace('\n',' ').replace('\r',' ')
            bodyText = jsonData["response"]['results'][i]['fields']['bodyText'].replace('\n',' ').replace('\r',' ')
            headline += bodyText
            label = jsonData["response"]['results'][i]['sectionName']
            if label not in labels:
                labels[label] = index
                index += 1
                # data.append({'label':labels[label],'Descript':headline})
            toAdd = str(labels[label]) + '||' + label + '||' + headline
            res_data.append(toAdd)


    return labels

def savelabel(labels):
    print("labels: ", labels)
    pd.DataFrame.from_dict(labels, orient='index').to_csv("hw3_label.csv")

'''
def getData(url):
    jsonData = requests.get(url).json()
    data = []
    labels = {}
    index = 0

    for i in range(len(jsonData["response"]['results'])):
        headline = jsonData["response"]['results'][i]['fields']['headline']
        bodyText = jsonData["response"]['results'][i]['fields']['bodyText']
        headline += bodyText
        label = jsonData["response"]['results'][i]['sectionName']
        if label not in labels:
            labels[label] = index
            index += 1
            # data.append({'label':labels[label],'Descript':headline})
        toAdd = str(labels[label]) + '||' + label + '||'+ headline
        data.append(toAdd)

    return data
'''

def readData(filename):
    df = pd.read_csv(filename, header=None)
    res_data = []
    index = 0
    nrows = df.shape[0]
    print("file rows: ", nrows)

    for i in range(1,nrows):
        res = df.iloc[i,1] + "||" + df.iloc[i,0] + "||" + df.iloc[i,2]
        res_data.append(res)

    #print("res_data: ", res_data[0])
    return res_data

def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        # _producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'),bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def save(all_news, file):
    label = [x.split('||')[0] for x in all_news]
    label_cat = [x.split('||')[1] for x in all_news]
    value = [x.split('||')[2] for x in all_news]
    val_new = {"label":label, "category":label_cat, "text":value}
    df = pd.DataFrame(val_new)
    df.to_csv(file, index=False)

if __name__ == "__main__":

    if len(sys.argv) != 5:
        print('Usage hw3_producer.py <api-key> <fromDate xxxx-x-x> <toDate xxxx-x-x> <train_data_flag>')
        print("train_data_flag 0: used as test data; 1: used as train data; 2: read from  publish.csv to publish; 3:save unique label relation")
        exit()

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    key = sys.argv[1]
    fromDate = sys.argv[2]
    toDate = sys.argv[3]
    trainFlag = sys.argv[4]
    filename1 = "train.csv"
    filename2 = "publish.csv"

    print("fromDate: ", fromDate)
    print("toDate: ", toDate)
    print("trainFlag: ", trainFlag)

    url = 'http://content.guardianapis.com/search?from-date=' + fromDate + '&to-date=' + toDate + '&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key=' + key
    #print("url: ", url)
    #all_news = getData(url)
    if(trainFlag == '0' or trainFlag == '1' or trainFlag == '3'):
        if trainFlag == '0':
            print("reading from guardian, publising test data....")
            all_news = getData(fromDate, toDate, key)
            if len(all_news) > 0:
                prod = connect_kafka_producer();
                for story in all_news:
                    print(json.dumps(story))
                    publish_message(prod, 'guardian2', story)
                    time.sleep(1)
                if prod is not None:
                    prod.close()
                save(all_news, filename2)
        elif trainFlag == '1':
            print("reading from guardian, save as train data....")
            all_news = getData(fromDate, toDate, key)
            if len(all_news) > 0:
                save(all_news, filename1)
        elif trainFlag == '3':
            print("reading from guardian, save label relationship....")
            labels = getDataforLabel(fromDate, toDate, key)
            savelabel(labels)
    elif trainFlag == '2':
        print("reading from publish.csv and publish....")
        read_news = readData(filename2)
        prod = connect_kafka_producer();
        for story in read_news:
            publish_message(prod, 'guardian2', story)
            time.sleep(1)
        if prod is not None:
            prod.close()


