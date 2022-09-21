import os
import pandas as pd
from azure.data.tables import TableServiceClient
import json
import time
from flaskr.yahoo_response_parser import get_player_week_key

class DBClient:

    # constructor for the DBClient class
    def __init__(self):
        self.table_service_client = TableServiceClient.from_connection_string(conn_str=os.environ["azure_connection_string"])

    # update league in DB
    def upsert_league(self, user_id, league_id, url):
        table_client = self.table_service_client.get_table_client("League")
        entity = {
            "PartitionKey": user_id,
            "RowKey": league_id,
            "url": url
        }
        table_client.upsert_entity(entity)

    # Query the League table based on user_id
    def get_leagues(self, user_id):
        table_client = self.table_service_client.get_table_client("League")
        entities = table_client.query_entities(f"PartitionKey eq '{user_id}'")
        entity_dict = {}
        for entity in entities:
            entity_dict[entity["RowKey"]] = entity
        return entity_dict

    # Save players_df, a pandas dataframe, to the Player Table with PartitionKey set to season
    # and RowKey set to player_key
    def save_player_data(self, players_df, season):
        table_client = self.table_service_client.get_table_client("Player")

        # upload each row to table_service["Player"]
        
        # time this operation
        start_time = time.time()
        operations = []
        for index, row in players_df.iterrows():
            entity = {
                "PartitionKey": season,
                "RowKey": self.get_row_key(season, row["player_key"], row["week"]),
                "Data": json.dumps(row.to_dict())
            }
            operations.append(("upsert", entity))

        # split operations into batches of 100
        operations_batches = [operations[i:i+100] for i in range(0, len(operations), 100)]
        for batch in operations_batches:
            table_client.submit_transaction(batch)
        print(f"Time to upload {len(players_df)} rows: {time.time() - start_time}")

    def get_player_data(self, row_keys, season):
        table_client = self.table_service_client.get_table_client("Player")
        row_key_conditions = [f"(RowKey eq '{row_key}')" for row_key in row_keys]
        # split into batches of 100
        row_key_conditions_batches = [row_key_conditions[i:i+100] for i in range(0, len(row_key_conditions), 100)]

        # time this operation
        start_time = time.time()

        entities = []
        for batch in row_key_conditions_batches:
            new_entities = table_client.query_entities(f"PartitionKey eq '{season}' and ({' or '.join(batch)})")
            entities.extend(new_entities)
        
        print(f"Time to query {len(row_keys)} rows in {len(row_key_conditions_batches)} batches: {time.time() - start_time}")

        # create player_stats_dicts from entitiy["Data"]
        player_stats_dicts = []
        for entity in entities:
            player_stats_dicts.append(json.loads(entity["Data"]))



        # create pandas dataframe from player_stats_dicts
        return pd.DataFrame(player_stats_dicts)
        

