from feast import Entity, Field, FeatureView, FileSource,ValueType,FeatureStore
from feast.types import Float64, Int32, Int64, String,Float32
from typing import List,Dict
from src.logger import logging
import pandas as pd




class Featurestore:

    def __init__(self):

        self.store = FeatureStore(repo_path=".")

    def featureview(self):
        logging.info('Creating feature views')
        user_entity = Entity(name="user_id",join_keys=['user_id'], description="Customer ID",value_type=ValueType.STRING)
        product_entity = Entity(name="product_id",join_keys=['product_id'], description="product ID",value_type=ValueType.STRING)
        session_entity = Entity(name='user_session',join_keys=['user_session'],description='session id',value_type=ValueType.STRING)
        category_entity = Entity(name='category_id',join_keys=['category_id'],description='category id',value_type=ValueType.STRING)

        filesource = FileSource(
                path="Artifacts/FeatureStore/train_data",
                event_timestamp_column="event_timestamp"
            )
        
        product_feature_view = FeatureView(
            name = 'product_features',
            entities = [product_entity],
            ttl=None,
            schema=[
                Field(name="price", dtype=Float32),
                Field(name="is_brand_missing", dtype=Int32),
                Field(name="normalised_price", dtype=Float64),
                Field(name="total_cart_of_product", dtype=Int64),
                Field(name="total_purchase_of_product", dtype=Int64),
                Field(name="total_view_of_product", dtype=Int64),
                Field(name="popularity_percentile", dtype=Float64),
                Field(name="product_cart_to_purchase_conversion_ratio", dtype=Float64),
                Field(name="view_to_purchase_conversion_rate_for_item", dtype=Float64),
                Field(name="view_to_purchase_ratio", dtype=Float64)
            ],
            online=True,
            source=filesource
        )

        category_feature_view = FeatureView(
            name = 'category_features',
            entities = [category_entity],
            ttl=None,
            schema=[
                Field(name='1_most_purchased_category',dtype=String),
                Field(name='2_most_purchased_category',dtype=String),
                Field(name='3_most_purchased_category',dtype=String),
                Field(name='brand',dtype=String)
            ],
            online=True,
            source=filesource
        )
                
        user_feature_view = FeatureView(
            name = 'customer_feature_view',
            entities=[user_entity],
            ttl=None,
            schema=[
                Field(name='Recency',dtype=Float64),
                Field(name='Frequency',dtype=Int64),
                Field(name='Monetary',dtype=Float64),
                Field(name="Num_purchase_per_2day", dtype=Int64),
                Field(name="total_cart_by_user", dtype=Int64),
                Field(name="total_purchase_by_user", dtype=Int64),
                Field(name="total_view_by_user", dtype=Int64),
                Field(name="Avg_purchase_price", dtype=Float32),
                Field(name="total_unique_product_purchases_by_user", dtype=Int64),
                Field(name="total_unique_product_add_cart_by_user", dtype=Int64),
                Field(name="view_to_purchase_time", dtype=Float64),
                Field(name="day", dtype=Int64),
                Field(name="month", dtype=Int64),
                Field(name="week", dtype=Int64),
                Field(name="hour_sin", dtype=Float64),
                Field(name="hour_cos", dtype=Float64),
                Field(name="minutes_sin", dtype=Float64),
                Field(name="minutes_cos", dtype=Float64),
                Field(name="second_sin", dtype=Float64),
                Field(name="second_cos", dtype=Float64),
            ],
            online=True,
            source=filesource
        )

        session_feature_view = FeatureView(
            name='session_feature_view',
            ttl=None,
            entities=[session_entity],
            schema=[
                Field(name="session_duration", dtype=Int64),
                Field(name="Avg_session_prices", dtype=Float64),
                Field(name="num_session_views", dtype=Int64),
                Field(name="num_session_cart", dtype=Int64),
                Field(name="num_session_purchase", dtype=Int64),
                Field(name="session_cart_to_purchase_ratio", dtype=Float64),
                Field(name="session_cart_abandonment_rate", dtype=Float64),
            ],
            online=True,
            source=filesource
        )

        user_product_feature_view = FeatureView(
            name='user_product_interaction',
            entities=[user_entity,product_entity],
            ttl=None,
            schema=[
                Field(name='interaction_score',dtype=Float64)
            ],
            online=True,
            source=filesource
        )

        self.store.apply([product_feature_view,user_feature_view,session_feature_view,user_product_feature_view
                         ,category_feature_view,user_entity,product_entity,session_entity,category_entity])
        
    def materialised_into_online_store(self,start_time,end_time):
            logging.info('Materialising offline store to  Redis online store')
            # Run materialization
            self.store.materialize(start_date=start_time, end_date=end_time)

    def get_feature_references(self):
            logging.info('getting feature references')
            feature_views = self.store.list_feature_views()
            feature_refs = []

            # Iterate over all feature views and their features
            for fv in feature_views:
                for feature in fv.features:
                    # Create fully qualified feature reference, e.g., "customer_profile:avg_purchase"
                    feature_ref = f"{fv.name}:{feature.name}"
                    feature_refs.append(feature_ref)
            print(feature_refs)
            return feature_refs
        
    def get_historical_data(self,data):
            logging.info('Fetching training data')
            feature_refs = self.get_feature_references()
            entity_df = data[['event_timestamp','user_id','product_id','user_session','category_id']]
            batch_size = 10000  # Adjust as needed
            entity_df_batches = [entity_df[i:i+batch_size] for i in range(0, len(entity_df), batch_size)]

            historical_features = pd.concat([
                self.store.get_historical_features(entity_df=batch, features=feature_refs).to_df()
                for batch in entity_df_batches
            ], ignore_index=True)

            
            historical_features.to_parquet('Artifacts\FeatureStore\\fs')

            logging.info('Read data from feast offline store and splitted data into Training , val and test split')

            return historical_features
        
    def get_online_features(self,entity_rows=List[Dict],feature_refs=List[str]):
            feature_vector = self.store.get_online_features(
                                    features=feature_refs,
                                    entity_rows=entity_rows)

            features_dict = feature_vector.to_dict()
            return features_dict
