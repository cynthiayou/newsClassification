from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pyspark
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.text import TextCollection
from time import sleep
import json, sys
import pandas as pd
import numpy as np
from os.path import join, exists
from datetime import date, timedelta
import string
from pyspark import Row
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib

def predict(kafka_data):

    print("begin spark map...")

    lines = kafka_data.map(lambda x: x[1].split("||")) \

    #print("type lines: ", type(lines))

    lines = lines.map(lambda x: Row(label=int(x[0]), category=x[1], text=x[2]))

    if lines.count() != 0:

        #wordsDF = spark.createDataFrame(lines)
        #print("lines before to df: ", lines.collect())
        pandas_df =  lines.toDF().toPandas()

        #print("lines after to df: ", pandas_df)
        print("begin predicting....")
        predict = model.predict(pandas_df['text'])

        #print("before add predict, pandas_df\n:", pandas_df)
        pandas_df['prediction'] = predict
        print("target and predict:\n", pandas_df[['category','label','prediction']])

        #print("type middle: ", type(pandas_df[['category', 'label', 'prediction']]))
        #total_prediction = total_prediction.append(pandas_df[['category', 'label', 'prediction']], ignore_index = True)
        #total_prediction = pd.concat([total_prediction, pandas_df[['category', 'label', 'prediction']]], axis= 0)

        # evalute
        print("begin evaluating...")
        print("batch accuracy: ", accuracy_score(pandas_df['label'], predict))
        #print("total accuracy: ", accuracy_score(total_prediction['label'], total_prediction['prediction']))


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print('Usage： hw3_predict_receiver.py <model_path> <topic(guardian2)>')
        #print('classify model:  0-MultinomialNB_train; 1-SGD_train; 2-LogisticRegression')
        exit()

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    model_path = sys.argv[1]
    topic = sys.argv[2]


    print("model_path: ", model_path)
    print("topic: ", topic)

    spark = SparkSession \
        .builder \
        .config("spark.executor.memory", "16g") \
        .config("spark.driver.memory","16g") \
        .config("spark.memory.offHeap.enabled",True) \
        .config("spark.memory.offHeap.size", "16g") \
        .appName("hw3_train_model") \
        .getOrCreate()

    print("loading model...")
    global model, total_prediction
    model = joblib.load(model_path)
    #total_prediction = pd.DataFrame()

    total_prediction = pd.DataFrame(columns=('category', 'label', 'prediction'))
    total_prediction['label'].astype('int')
    total_prediction['prediction'].astype('int')


    print("consuming data....")

    # batch interval 1 second
    ssc = StreamingContext(spark.sparkContext, 10)

    brokers = "localhost:9092"
    print("ssc:", ssc)
    print("topic", topic)
    print("brokers ", brokers)
    group_id = "hw3"
    partition = 0
    start = 0


    kafka_data = KafkaUtils.createDirectStream(ssc, [topic],
                        kafkaParams={"metadata.broker.list": brokers, "group.id" : group_id},
                        fromOffsets={TopicAndPartition(topic, partition):start})

  

    kafka_data.foreachRDD(predict)


    ssc.start()
    ssc.awaitTermination()
    spark.stop()

    if total_prediction.shape[0] != 0:
        print("final total accuracy: ", accuracy_score(total_prediction['target'], total_prediction['prediction']))





