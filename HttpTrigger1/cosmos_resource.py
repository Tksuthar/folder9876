import azure.cosmos.cosmos_client as cosmos_client
from azure.cosmos import PartitionKey
from . import config
import logging


class CosmosResources:

    def create_cosmos_database(self, database_name):
        client = cosmos_client.CosmosClient(config.Config.host, {'masterKey': config.Config.key})
        db_client = client.create_database(database_name)
        return db_client

    try:
        def get_cosmos_database(self, database_name):
            client = cosmos_client.CosmosClient(config.Config.host, {'masterKey': config.Config.key})
            db_client = client.get_database_client(database_name)
            return db_client
            
    except Exception as e:
        logging.error("Database does not exists", e)

    def create_cosmos_container(self, db_client, container_name, pkey):
        container_client = db_client.create_container_if_not_exists(container_name, partition_key=PartitionKey(path=pkey, kind='Hash'))
        return container_client

    try:
        def get_cosmos_container(self, db_client, container_name):
            container_client = db_client.get_container_client(container_name)
            return container_client

    except Exception as e:
        logging.error("Container does not exists", e)

# if __name__ == "__main__":
#     try:
#         create_containers = CosmosResources()
#         transactional_db_client = create_containers.create_cosmos_database(config.Config.transactional_database_name)
#         input_container = create_containers.create_cosmos_container(transactional_db_client, config.Config.input_data, '/name')
#         predicted_container = create_containers.create_cosmos_container(transactional_db_client, config.Config.predicted_data, '/name')

#         analytical_db_client = create_containers.create_cosmos_database(config.Config.analytical_database_name)
#         output_container = create_containers.create_cosmos_container(analytical_db_client, config.Config.output_data, '/name')
#         print("Cosmos DB databases and containers are created")
#         logging.info("Cosmos DB databases and containers are created")

#     except Exception as e:
#         logging.error("Exception while creating databases and containers")