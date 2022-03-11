import json
from . import data_generator
import logging
import requests


class DataPredictor:

    try:
        def data_predictions(self, input_container, predicted_container):
            tf = data_generator.DataGenerator()
            # REST_API_URL = 'https://api.powerbi.com/beta/e4e34038-ea1f-4882-b6e8-ccd776459ca0/datasets/f2928cb7-5948-49a0-9943-02a6a0aad1f9/rows?key=ZuZ2046ZdZmOMQeBKnXN8wu756Dz6RNtl5HA5UYPpEVrsSWecEicXQhukc1gTXxfHmy%2FO3p1AWUnKqPsoLFH%2BQ%3D%3D'
            query_result = input_container.query_items(query="SELECT * FROM r", enable_cross_partition_query=True)

            for res in query_result:
                amb_temp = tf.get_ambient_temp(res['main']['temp'])
                tf_json = tf.data_generator_function(amb_temp)
                data_json = bytes(tf_json, encoding='utf-8')
                dictionary = json.loads(tf_json)
                dictionary[0]['id'] = res['id']

                predicted_container.create_item(dictionary[0])
                # requests.post(REST_API_URL, data_json)
            logging.info("Data predicted successfully")

    except Exception as e:
        logging.error("Exception while predicting the data", e)
