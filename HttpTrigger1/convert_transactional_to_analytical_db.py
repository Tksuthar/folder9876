import random
import logging


class ConvertTransactionalToAnalyticalDB:
    def converter(self, predicted_container, output_container):

        try:
            logging.info("Converting transactional database into analytical database ...")
            query_result = predicted_container.query_items(query="SELECT * FROM r", enable_cross_partition_query=True)
            for res in query_result:

                w1_diff = abs(res['temp_lv_w1_pred'] - res['temp_lv_w1'])
                w2_diff = abs(res['temp_lv_w2_pred'] - res['temp_lv_w2'])
                temp_hv_diff = abs(res['temp_hv_pred'] - res['temp_hv'])
                oti_diff = abs(res['OTI_temp_pred'] - res['OTI_temp'])

                res['temp_lv_w1_ano'] = 0
                res['temp_lv_w2_ano'] = 0
                res['temp_hv_ano'] = 0
                res['OTI_temp_ano'] = 0

                if w1_diff >= (res['temp_lv_w1'] / 4):
                    res['temp_lv_w1_ano'] = 1
                if w2_diff >= (res['temp_lv_w2'] / 4):
                    res['temp_lv_w2_ano'] = 1
                if temp_hv_diff >= (res['temp_hv'] / 4):
                    res['temp_hv_ano'] = 1
                if oti_diff >= (res['OTI_temp'] / 4):
                    res['OTI_temp_ano'] = 1
 
                res['RUL'] = random.randint(1, 100)
                res['fault_p_1D'] = random.randint(1, 100)
                res['fault_p_2D'] = random.randint(1, 100)
                output_container.create_item(res)
            logging.info("Converted transactional database into analytical database")

        except Exception as e:
            logging.error("Exception while converting transactional database into analytical database", e)



#     convert_db = ConvertTransactionalToAnalyticalDB()
#     create_analytical_db = cosmos_resource.CosmosResources()
#     transactional_db_client = create_analytical_db.get_cosmos_database(Config.transactional_database_name)
#     predicted_container = create_analytical_db.get_cosmos_container(transactional_db_client, Config.predicted_data)
#     analytical_db_client = create_analytical_db.get_cosmos_database(Config.analytical_database_name)
#     output_container = create_analytical_db.get_cosmos_container(analytical_db_client, Config.output_data)
#     convert_db.converter(predicted_container, output_container)