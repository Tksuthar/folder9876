import logging
from . import cosmos_resource
from . import config
from . import data_predictor
from . import convert_transactional_to_analytical_db
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')



    #code
    create_containers = cosmos_resource.CosmosResources()
    transactional_db_client = create_containers.get_cosmos_database(config.Config.transactional_database_name)
    input_container = create_containers.get_cosmos_container(transactional_db_client,config.Config.input_data)
    predicted_container = create_containers.get_cosmos_container(transactional_db_client,config.Config.predicted_data)

    analytical_db_client = create_containers.get_cosmos_database(config.Config.analytical_database_name)
    output_container = create_containers.get_cosmos_container(analytical_db_client,config.Config.output_data)

    predict_data = data_predictor.DataPredictor()
    predict_data.data_predictions(input_container, predicted_container)

    convert_db = convert_transactional_to_analytical_db.ConvertTransactionalToAnalyticalDB()
    convert_db.converter(predicted_container, output_container)

    
    return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
    )
