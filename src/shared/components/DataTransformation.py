from src.entity.config_entity import DataTransformationConfig
from src.utils.commons import spark_session
from src.logger import logging
import pandas as pd
import findspark
findspark.init()
 
from pyspark.sql import types,Window

from pyspark.sql.types import StructField,StructType,FloatType
from pyspark.ml.feature import FeatureHasher, VectorAssembler,StringIndexer,StandardScaler
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml import PipelineModel
from datetime import datetime
import os
from glob import glob
import math
from src.cloud_storage.featurestore import Featurestore
from dateutil.relativedelta import relativedelta




class DataTransformation:

    def __init__(self,config = DataTransformationConfig):

        self.config = config
        self.spark = spark_session()

    def read_schema(self):

        fields = []

        logging.info('Reading schema from schema.yaml')
        logging.info(f'schema {self.config.schema}')

        for  field_dict in self.config.schema['fields']:
            field_type = getattr(types, field_dict['type'])()
            fields.append(StructField(field_dict['name'], field_type))

        return StructType(fields)
    
    def load_data(self):
        try:
            parquet_path = self.config.source_parquetpath  # Path for Parquet files
            
            # Check if the Parquet file exists
            if os.path.exists(parquet_path):
                logging.info(f'Parquet file exists, loading data from {parquet_path}')
                return parquet_path  # Return the path to the existing Parquet file
            else:
                # Load data from CSV if Parquet file is not available
                logging.info(f'Loading data from {self.config.source_datapath}')
                datapath = glob(self.config.source_datapath)  # List of CSV files
                self.schema = self.read_schema()  # Read or define the schema
                

                df = None
                for path in datapath:
                    logging.info(f'data path :{path}')
                    # Load each CSV file and combine them into a DataFrame
                    temp_df = self.spark.read.csv(path, schema=self.schema, header=True)
                    df = temp_df if df is None else df.union(temp_df)
                
                    logging.info(f'number of data point {df.count()}')

                # Order by event_time if needed
                df = df.orderBy('event_time')

                # Save the processed DataFrame to Parquet for future use
                logging.info(f'Saving processed data as Parquet at {parquet_path}')
                # Remove "remove_from_cart" events
                df = df.filter(df['event_type'] != 'remove_from_cart')
                 # (Optional) Sample the data if needed (here, 1% sampling)
                logging.info('sampling dataset')
                df.write.parquet(parquet_path,mode='overwrite')

                # Return the path to the newly saved Parquet file
                return parquet_path

        except Exception as e:
            logging.error(f'Could not load data: {e}')
            return None
        
    def train_test_split(self, data):
        """
        Splits the data into training and test sets based on user interactions and event time.

        Parameters:
            data (str): data file.
            
        Returns:
            train_df (DataFrame): Training data.
            test_df (DataFrame): Test data.
        """
        logging.info('Splitting data into training and test sets')
        try:


            # Compute total interactions for each user
            user_total_interactions = data.groupBy("user_id").agg(F.count("*").alias("total_interactions"))

            # Join the total interactions back to the original DataFrame
            data = data.join(user_total_interactions, on="user_id")

            # Filter out users with low interactions (e.g., fewer than 8\10 interactions)
            data = data.filter(F.col('total_interactions') > 10)
            logging.info('Filtered out users with low interactions')

            # Assign interaction ranks within each user based on event_time
            user_window = Window.partitionBy("user_id").orderBy("event_timestamp")
            df_with_rank = data.withColumn("interaction_rank", F.row_number().over(user_window))

            # Define train cutoff (e.g., first 80% of interactions for training)
            train_cutoff = 0.8

            # Create the split column:
            # - Otherwise, interactions within the first 70% go to training, the rest to test.
            df_with_rank = df_with_rank.withColumn(
                "split",
                F.when(
                    (df_with_rank["interaction_rank"] <= (df_with_rank["total_interactions"] * train_cutoff)),
                    "train"
                ).otherwise("test")
            )

            # Filter data into train and test DataFrames, and drop the helper column 'total_interactions'
            train_df = df_with_rank.filter("split = 'train'").drop('total_interactions')
            test_df = df_with_rank.filter("split = 'test'").drop('total_interactions')
            logging.info('Data successfully split into train and test sets')
            return train_df, test_df
        except Exception as e :
            logging.error(f'Could not split data : {e}')
    
    def split_event_types(self, data):
        return {
            'cart': data.filter(F.col('event_type') == 'cart'),
            'purchase': data.filter(F.col('event_type') == 'purchase'),
            'view': data.filter(F.col('event_type') == 'view')
        }
    
        
    def clean_data(self, data):
        try:
            logging.info('cleaning dataset')
            data = data.drop('category_code').fillna({'brand': 'UNKNOWN'})
            data = data.withColumn('is_brand_missing', F.when(F.col('brand') == 'UNKNOWN', 1).otherwise(0))
            data = data.dropna(subset=['user_session']).dropDuplicates()
            data = data.withColumn('price', F.abs(F.col('price')))
            return data
        except Exception as e:
            logging.error(f' could not clean data because of {e}')

    def compute_two_day_purchases(self, event_dfs, data):
        logging.info('Creating feature: two_day_purchase')
        purchase_df_per_2day = event_dfs['purchase'].withColumn('two_day_purchase',
            F.from_unixtime(F.floor(F.unix_timestamp(F.col('event_time')) / (2 * 86400)) * (2 * 86400)))
        purchase_df_per_2day = purchase_df_per_2day.groupBy('user_id', 'two_day_purchase')\
            .agg(F.count('*').alias('Num_purchase_per_2day'))
        purchase_df_per_2day = purchase_df_per_2day.withColumnRenamed("user_id", "train_user_id")
        
        data = data.join(
            purchase_df_per_2day,
            (data['user_id'] == purchase_df_per_2day['train_user_id']) &
            (F.floor(F.unix_timestamp(data['event_time']) / (2*86400)) ==
             F.floor(F.unix_timestamp(purchase_df_per_2day['two_day_purchase']) / (2 * 86400))),
            how='left'
        ).drop('two_day_purchase', 'train_user_id').na.fill({'Num_purchase_per_2day': 0})
        return data
    
    def compute_rfm_features(self, event_dfs, data):
        logging.info('Creating RFM features')
        reference_time = data.select(F.max(F.col('event_time')).cast('long')).collect()[0][0]
        last_purchase_time = event_dfs['purchase'].groupBy('user_id').agg(F.max('event_time').alias('last_purchase_time'))
        recency_df = last_purchase_time.withColumn('Recency', (F.lit(reference_time) - F.col('last_purchase_time').cast('long'))/86400).drop('last_purchase_time')
        data = data.join(recency_df, on='user_id', how='left')
        min_event_time = data.select(F.min('event_time')).collect()[0][0]
        max_recency_value = (reference_time - min_event_time.timestamp()) / 86400
        data = data.na.fill({'Recency': max_recency_value})
        
        frequency_df = event_dfs['purchase'].groupBy('user_id').agg(F.count(F.col('event_type')).alias('Frequency'))
        data = data.join(frequency_df, on='user_id', how='left').na.fill({'Frequency': 0})
        
        monetary_value = event_dfs['purchase'].groupBy('user_id').agg(F.sum(F.col('price')).alias('Monetary'))
        data = data.join(monetary_value, on='user_id', how='left').na.fill({'Monetary': 0})
        
        return data
    
    def compute_user_aggregates(self, event_dfs, data):
        logging.info('Computing user-level aggregates')
        for event, event_df in event_dfs.items():
            count_col = f'total_{event}_by_user'
            agg_df = event_df.groupBy('user_id').agg(F.count('event_type').alias(count_col))
            data = data.join(agg_df, on='user_id', how='left').na.fill({count_col: 0})
        return data
    
    def compute_avg_purchase_price(self, event_dfs, data):
        logging.info('Creating feature: Avg_purchase_price')
        avg_purchase_price = event_dfs['purchase'].groupBy('user_id').agg(F.format_number(F.mean('price'), 2).cast(FloatType()).alias('Avg_purchase_price'))
        avg_purchase_price = avg_purchase_price.withColumnRenamed("user_id", "train_user_id")
        data = data.join(avg_purchase_price, data['user_id'] == avg_purchase_price['train_user_id'], how='left')
        return data.drop('train_user_id').na.fill({'Avg_purchase_price': 0})
    
    def create_user_unique_product_interaction_feature(self,event_dfs,data):
        '''
        create features that shows the number of unique product the users has added to cart of purchased
        '''
        logging.info('Combined feature: total number of products purchased to main dataframe')
        total_purchases_by_user_across_items = event_dfs['purchase'].groupBy('user_id').agg(
            F.countDistinct('product_id').alias('total_unique_product_purchases_by_user')
        )
        data = data.join(
            total_purchases_by_user_across_items,
            on='user_id',
            how='left'
        ).na.fill({'total_unique_product_purchases_by_user': 0})

         # Total number of products added to the cart
        logging.info('Combined feature: total number of unique products added to cart to main dataframe')
        total_add_cart_by_user_across_items = event_dfs['cart'].groupBy('user_id').agg(
            F.countDistinct('product_id').alias('total_unique_product_add_cart_by_user')
        )
        data = data.join(
            total_add_cart_by_user_across_items,
            on='user_id',
            how='left',
        ).na.fill({'total_unique_product_add_cart_by_user': 0})

        return data
    
    def time_btw_view_purchase(self,event_dfs,data):

        '''
        Creates a feature that calculates the time spent between a product view event and its corresponding purchase event for each user.

        This feature represents the time difference (in days) between when a user views a product and subsequently makes a purchase. It helps to assess user engagement and product interest prior to purchase.
        '''
        logging.info('combined feature: Time spent between view and purchase to main dataframe')
        view_time = event_dfs['view'].groupBy('user_id','product_id').agg(F.min('event_time').alias('view_time'))
        purchase_time = event_dfs['purchase'].groupBy('user_id','product_id').agg(F.min('event_time').alias('purchase_time'))
        # Join views and purchases on user_id and product_id
        time_diff_df = view_time.join(purchase_time, ["user_id", "product_id"], "inner")
        # Calculate time difference between view and purchase
        time_diff_df = time_diff_df.withColumn(
            "view_to_purchase_time", ((F.col("purchase_time").cast('long')) - (F.col("view_time").cast('long')))/86400
                                    )
        time_diff_df = time_diff_df.select("user_id", "product_id", "view_to_purchase_time")
        data = data.join(
            time_diff_df,
            on= ['user_id','product_id'],
            how = 'left'
        ).na.fill({'view_to_purchase_time':0})

        return data
    
    def normalised_price(self,data):
        '''
        Creates a normalized price feature that compares the price of a product to the average price of other products within the same category.
    
        This feature helps to assess how expensive a product is relative to others in the same category. A higher normalized price indicates that the product is more expensive than the category average, while a lower value indicates it is cheaper.
        '''
        # normalized price
        logging.info('creating feature : normalised_product_price')
        window_cat = Window.partitionBy('category_id')
        data = data.withColumn('avg_cat_price',F.avg('price').over(window_cat))
        data = data.withColumn('normalised_price',F.col('price')/F.col('avg_cat_price')).drop('avg_cat_price')
        
        return data
    
    def most_purchased_category(self,event_dfs,data):
        '''
        Creates features for the top three most purchased categories for each user.

         This feature identifies the top three categories a user has purchased from, based on the frequency of purchases, 
        and provides a fallback category in case there are missing values. It helps understand user preferences 
        by showing the most relevant categories they engage with.
        '''
        # groupby user id and category and create a window specification to get the top three brands
        logging.info('creating most purchase category')
        category_count = event_dfs['purchase'].groupBy('user_id','category_id').agg(F.count('category_id').alias('category_count'))
        window_spec = Window.partitionBy('User_id').orderBy(F.desc('category_count'))
        ranked_categorys = category_count.withColumn('rank' ,F.row_number().over(window_spec))
        top_categorys = ranked_categorys.filter(F.col("rank") <= 3)
        top_categorys = top_categorys.groupBy("user_id").pivot("rank", [1, 2, 3]).agg(F.first("category_id"))
        # rename the columns
        top_brands = top_categorys.select(
            F.col("user_id"),
            F.col("1").alias("1_most_purchased_category"),
            F.col("2").alias("2_most_purchased_category"),
            F.col("3").alias("3_most_purchased_category")
        )
        # Get the most purchased brand for each user
        most_purchased_category = category_count.withColumn('rank', F.row_number().over(Window.partitionBy('user_id').orderBy(F.desc('category_count'))))
        most_purchased_category = most_purchased_category.filter(F.col('rank') == 1).select('user_id', 'category_id')
        # Join the top brands with the most purchased brand
        top_category_with_most_purchased = top_brands.join(most_purchased_category, on='user_id', how='left')
        top_category_with_most_purchased = top_category_with_most_purchased.withColumnsRenamed({'user_id':'train_user_id','category_id':'preferred_category'})
        # Fill missing values with the user's most purchased brand
        # Replace missing values with the preferred brand
        top_brands_with_most_purchased = top_category_with_most_purchased.withColumn(
            '1_most_purchased_category',
            F.when(F.col('1_most_purchased_category').isNull(), F.col('preferred_category')).otherwise(F.col('1_most_purchased_category'))
        ).withColumn(
            '2_most_purchased_category',
            F.when(F.col('2_most_purchased_category').isNull(), F.col('preferred_category')).otherwise(F.col('2_most_purchased_category'))
        ).withColumn(
            '3_most_purchased_category',
            F.when(F.col('3_most_purchased_category').isNull(), F.col('preferred_category')).otherwise(F.col('3_most_purchased_category'))
        )
        data = data.join(
            top_brands_with_most_purchased,
            data['user_id'] == top_brands_with_most_purchased['train_user_id'],
            how = 'left'
        ).drop('train_user_id','preferred_category').na.fill({'1_most_purchased_category':'No purchase',
                                                        '2_most_purchased_category':'No purchase',
                                                        '3_most_purchased_category' : 'No purchase'})
        
        return data
        
    def session_features(self,event_dfs,data):
            
            '''
            Creates various session-based features for user interactions.

            This function calculates multiple session-level features for each user-session combination, such as session duration, 
            average session price, number of views, number of items added to the cart, number of purchases, and cart abandonment rate.
            
            The features created include:
            - `user_session_duration`: Duration of a user's session.
            - `Avg_session_prices`: Average price of items in the user's session.
            - `num_session_views`: Number of products viewed in a session.
            - `num_session_cart`: Number of products added to the cart in a session.
            - `num_session_purchase`: Number of purchases made in a session.
            - `session_cart_to_purchase_ratio`: Ratio of items added to the cart to purchases in a session.
            - `session_cart_abandonment_rate`: Rate of cart abandonment, calculated as 1 - (purchases / cart additions).
            '''
            # duration of users session
            logging.info('creating feature : user_session_duration')
            session_duration = data.groupBy('user_id','user_session').agg(
                F.min(F.col('event_time')).alias('session_start'),
                F.max(F.col('event_time')).alias('session_end')
            ).withColumn('session_duration', F.unix_timestamp('session_end') - F.unix_timestamp('session_start'))
            # combine 
            data = data.join(
                session_duration,
                on = ['user_id','user_session'],
                how='left'

            ).drop('session_start','session_end')
            logging.info('combined feature(user_session_duration) to main dataframe')

            # avearge session price for view,cart and purchase
            logging.info('creating feature : session_avg_intent_prices')
            session_avg_price = data.groupBy('user_id','user_session').agg(F.round(F.mean('price'),2).alias('Avg_session_prices'))
            data = data.join(
                session_avg_price,
                on = ['user_id','user_session'],
                how='left'
            ).na.fill({'Avg_session_prices':0})

            # total number of product viewed during a session
            logging.info('creating feature : num_session_views')
            num_session_view = event_dfs['view'].groupBy('user_id','user_session').agg(F.count('event_type').alias('num_session_views'))
            # combine dataframe
            data = data.join(
                num_session_view,
                on = ['user_id','user_session'],
                how='left'
            ).na.fill({'num_session_views' : 0})

            # number of product added to cart in a sesiion
            logging.info('creating feature : num_session_cart')
            num_session_cart = event_dfs['cart'].groupBy('user_id','user_session').agg(F.count('event_type').alias('num_session_cart'))
            # combine dataframe
            data = data.join(
                num_session_cart,
                on = ['user_id','user_session'],
                how='left'
            ).na.fill({'num_session_cart' : 0})

            # session purchase
            logging.info('creating feature : num_session_purchase')
            num_session_purchase = event_dfs['purchase'].groupBy('user_id', 'user_session').agg(F.count('event_type').alias('num_session_purchase'))
            data = data.join(
                num_session_purchase,
                on=['user_id', 'user_session'],
                how='left'
            ).na.fill({'num_session_purchase':0})

            # cart-to-purchase ration
            logging.info('creating feature : session_cart_to_purchase_ratio')
            data = data.withColumn('session_cart_to_purchase_ratio', F.when(F.col('num_session_purchase') == 0 , 0).otherwise(
                F.col('num_session_cart') / F.col('num_session_purchase')
            ))
            data = data.withColumn('session_cart_to_purchase_ratio', F.round(F.col('session_cart_to_purchase_ratio'), 2))

            # Calculate Cart Abandonment Rate and round to two decimal points
            logging.info('creating feature : session_cart_abondonment_rate')
            data = data.withColumn('session_cart_abandonment_rate', 
                                    F.when(F.col('num_session_cart') == 0, 0)  # Handle case where no products were added to the cart
                                    .otherwise(1 - (F.col('num_session_purchase') / F.col('num_session_cart'))))

            data = data.withColumn('session_cart_abandonment_rate', F.round(F.col('session_cart_abandonment_rate'), 2)) 

            return data
        
    
    def compute_product_aggregates(self, event_dfs, data):
        logging.info('Computing prooduct-level aggregates')
        for event, event_df in event_dfs.items():
            count_col = f'total_{event}_of_product'
            agg_df = event_df.groupBy('product_id').agg(F.count('event_type').alias(count_col))
            data = data.join(agg_df, on='product_id', how='left').na.fill({count_col: 0})
        return data
    
    def product_popularity_percent(self,data):
        '''
            Creates a popularity percentile for each product based on its views within each category.

            This function calculates a product's popularity percentile within its category, 
            providing a measure of how popular a product is compared to other products within the same category.            
            The formula for `popularity_percentile` is:
                popularity_percentile = total_product_views / max_view_category (if max_view_category > 0)
        '''

        # Compute max view_count per category
        logging.info('createing feature : popularity percentile')
        window_cat = Window.partitionBy('category_id')
        data = data.withColumn(
            "max_view_category",
            F.max("total_view_of_product").over(window_cat)
        )
        # Compute popularity percentile
        data = data.withColumn(
            "popularity_percentile",
            F.when(F.col("max_view_category") > 0, F.col("total_view_of_product") / F.col("max_view_category")).otherwise(0)
        ).drop("max_view_category")

        return data
    
    def product_features(self,data):
            
            '''
            Creates product-related features for conversion rates and ratios based on product views, cart additions, and purchases.

            This function calculates various conversion rates and ratios to assess product performance:
            1. **Product Cart-to-Purchase Conversion Ratio**: The ratio of products added to the cart to products purchased.
            2. **View-to-Purchase Conversion Rate**: The conversion rate from product views to purchases.
            3. **View-to-Purchase Ratio**: The ratio of product views to product purchases.
            '''
        # product cart  purchase conversion rate 
            logging.info('creating feature :  product cart_purchase_conversion rate')
            data = data.withColumn('product_cart_to_purchase_conversion_ratio', F.when(F.col('total_purchase_of_product') == 0 , 0).otherwise(
                F.col('total_cart_of_product') / F.col('total_purchase_of_product')
            ))
            data = data.withColumn('product_cart_to_purchase_conversion_ratio', F.round(F.col('product_cart_to_purchase_conversion_ratio'), 2))

            # conversion rate from view to purchase
            logging.info('creating feature :  view to purchase_conversion rate')
            data = data.withColumn('view_to_purchase_conversion_rate_for_item', F.when(F.col('total_view_of_product') == 0,0).otherwise(
                                                                         F.col('total_purchase_of_product')/F.col('total_view_of_product')))
            
            data = data.withColumn('view_to_purchase_conversion_rate_for_item', F.round(F.col('product_cart_to_purchase_conversion_ratio'), 2))

            # view_to_purchase_ratio
            logging.info('creating feature :  view_to_purchase ratio')
            data = data.withColumn('view_to_purchase_ratio', F.when(F.col('total_purchase_of_product') == 0 , 0).otherwise(
                                                     F.col('total_view_of_product')/F.col('total_purchase_of_product')))
            data = data.withColumn('view_to_purchase_ratio', F.round(F.col('view_to_purchase_ratio'), 2))

            return data
    
    def user_product_interaction(self,event_dfs,data):

        '''
        Creates features to track user-product interactions, including views, cart additions, and purchases.

        This function computes the following features:
        1. **User-Product View Count**: The count of views for each product by each user.
        2. **User-Product Cart Count**: The count of cart additions for each product by each user.
        3. **User-Product Purchase Count**: The count of purchases for each product by each user.
        4. **Interaction Weight**: Assigns different weights to user actions (view, cart, purchase).
        5. **Interaction Score**: A weighted sum of the user-product interactions (view, cart, purchase).
        '''

        #user_product_view_count
        logging.info('creating feature : user_product_view')
        user_product_view = event_dfs['view'].groupBy('user_id','product_id').agg(F.count('event_type').alias('user_product_view'))
        data = data.join(
            user_product_view,
            on=['user_id','product_id'],
            how='left'
        ).na.fill({'user_product_view':0})
        #user_product_add to cart _count
        logging.info('creating feature : user_product_cart')
        user_product_cart = event_dfs['cart'].groupBy('user_id','product_id').agg(F.count('event_type').alias('user_product_cart'))
        data = data.join(
            user_product_cart,
            on=['user_id','product_id'],
            how='left'
        ).na.fill({'user_product_cart':0})
        #user_product_add to cart _count
        logging.info('creating feature : user_product_purchase')
        user_product_purchase = event_dfs['purchase'].groupBy('user_id','product_id').agg(F.count('event_type').alias('user_product_purchase'))
        data = data.join(
            user_product_purchase,
            on=['user_id','product_id'],
            how='left'
        ).na.fill({'user_product_purchase':0})
        
        logging.info('creating feature: interaction_weight')
        data = data.withColumn('interaction_weight' , F.when(
                F.col('event_type') == 'view',0.3).when(
                    F.col('event_type') == 'cart' ,0.5
                ).when(
                    F.col('event_type') == 'purchase',1
                )
                  ).drop('event_type')
        # interaction score 
        logging.info('creating feature : interaction score')
        data = data.withColumn('interaction_score' ,
                                F.col('user_product_view') * F.col('interaction_weight') + 
                               F.col('user_product_cart')*F.col('interaction_weight') + 
                               F.col('user_product_purchase') * F.col('interaction_weight')).drop('interaction_rank','interaction_weight'
                                                                                                  ,'user_product_view',
                                                                                                  'user_product_cart','user_product_purchase')
        return data
    
    def time_features(self,data):
        
        '''
        Creates temporal features from the `event_time` column and generates cyclic time features.

        This function generates the following features:
        **Cyclic Hour Features**: Sine and cosine transformations of the hour to represent time cyclically.
        **Cyclic Minute Features**: Sine and cosine transformations of the minute to represent time cyclically.
        **Cyclic Second Features**: Sine and cosine transformations of the second to represent time cyclically.
        '''
        
        logging.info('creating feature : Temporal features')
            
        data = data.withColumn('day',F.day(F.col('event_time')))
        data = data.withColumn('month',F.month(F.col('event_time')))
        data = data.withColumn('week',F.dayofweek(F.col('event_time')))
        data = data.withColumn('hour',F.hour(F.col('event_time')))
        data  = data.withColumn('minutes',F.minute(F.col('event_time')))
        data = data.withColumn('seconds',F.second(F.col('event_time')))
        
        # add cyclic time features 
        data = data.withColumn(
            'hour_sin' , F.sin(2 * math.pi * F.col('hour') / 24)
        ).withColumn('hour_cos',F.cos(2 * math.pi * F.col('hour') / 24))
        data = data.withColumn(
            'minutes_sin' , F.sin(2 * math.pi * F.col('minutes') / 24)
        ).withColumn('minutes_cos',F.cos(2 * math.pi * F.col('minutes') / 24))
        data = data.withColumn(
            'second_sin',F.sin(2 * math.pi * F.col('seconds') / 24)
        ).withColumn('second_cos',F.cos(2 * math.pi * F.col('seconds') / 24))
        data = data.orderBy('event_time').drop('hour','minutes','seconds').withColumnRenamed('event_time','event_timestamp')
        return data
    
    def feature_engineering(self):
        try:
            
           # logging.info(f'Starting feature engineering')
            data_path = self.load_data()
            data = self.spark.read.parquet(data_path)
            data = self.clean_data(data)
            event_dfs = self.split_event_types(data)
            data = self.compute_two_day_purchases(event_dfs,data)
            data = self.compute_rfm_features(event_dfs,data)
            data = self.compute_user_aggregates(event_dfs,data)
            data = self.compute_avg_purchase_price(event_dfs,data)
            data = self.create_user_unique_product_interaction_feature(event_dfs,data)
            data = self.time_btw_view_purchase(event_dfs,data)
            data = self.normalised_price(data)
            data = self.most_purchased_category(event_dfs,data)
            data = self.session_features(event_dfs,data)
            data = self.compute_product_aggregates(event_dfs,data)
            data = self.product_popularity_percent(data)
            data = self.product_features(data)
            data = self.user_product_interaction(event_dfs,data)
            data = self.time_features(data)
            data.write.parquet(self.config.train_datapath,mode='overwrite')
            logging.info(f'feature engineering complected')
            
            logging.info('uploading data into feature store')
            Fs = Featurestore()
            Fs.featureview()
            #  materialial train data (used as historical features) from offline store to online store
            data = self.spark.read.parquet('Artifacts\FeatureStore\\train_data')
            end_time = data.select(F.max("event_timestamp")).collect()[0][0]
            start_time = end_time - relativedelta(months=2)
            Fs.materialised_into_online_store(start_time=start_time,end_time=end_time)
            logging.info('data materialised into feature store')
            return data

        except Exception as e:
            logging.error(f'COuld not perform feature engineering {e}')
        
    
    def feature_transformation(self, data) :
        try:
            logging.info('Performing feature transformation')

            # read data
            data =  self.spark.read.parquet(self.config.train_datapath)

            # perfrom train test split
            train_data,test_data = self.train_test_split(data)

            # Define transformations
            features_to_hash = ['user_id', 'product_id', 'category_id', 'brand', 'user_session','1_most_purchased_category',
                                '2_most_purchased_category','3_most_purchased_category']
            
            hashingTFs = [FeatureHasher(inputCols=features_to_hash, outputCol="categorical_hash_values", numFeatures=8000)]
            user_id_indexer = StringIndexer(inputCol='user_id', outputCol='user_id_index',handleInvalid='keep')
            product_id_indexer = StringIndexer(inputCol='product_id', outputCol='product_id_index',handleInvalid='keep')
            assembler = VectorAssembler(inputCols=["Recency", "Frequency", "Monetary"], outputCol="unscaled_features")
            scaler = StandardScaler(inputCol="unscaled_features", outputCol="RFM_scaled_features", withStd=True, withMean=True)
            kmeans = KMeans(k=4, seed=1, featuresCol="RFM_scaled_features", predictionCol="RFM_user_cluster")
 
            pipeline = Pipeline(stages= [user_id_indexer,product_id_indexer] +hashingTFs + [assembler,scaler,kmeans] )
            # Fit on train and transform all sets
            logging.info('fitting train data')
            pipeline_model = pipeline.fit(train_data)
            # Save the pipeline model for inference
            pipeline_model.save(self.config.trans_pipeline_model_path)
            logging.info(f"Pipeline model saved at: {self.config.trans_pipeline_model_path}")

            logging.info('transforming datasets')
            features_to_drop = features_to_hash + ["unscaled_features","Recency", "Frequency", "Monetary"]
    
            train_data = pipeline_model.transform(train_data).drop(*features_to_drop)
            test_data = pipeline_model.transform(test_data).drop(*features_to_drop)

            logging.info(f"Train data schema: {train_data.printSchema()} \n")
            logging.info(f"Test data schema: {test_data.printSchema()}\n")

            # Save transformed data
            logging.info(f' saving train data into train path {self.config.train_transformed_datapath}')
            train_data.write.parquet(self.config.train_transformed_datapath,mode='overwrite')
            logging.info(f"saving train data into test path :  {self.config.test_transformed_datapath}")
            test_data.write.parquet(self.config.test_transformed_datapath,mode='overwrite')
            logging.info('saved test transformed data')

            logging.info('Feature transformation completed')
            self.spark.stop()
        except Exception as e:
            logging.error(f'Error during feature transformation: {e}')

    def transform_for_inference(self, new_data):
        try:
            logging.info('Loading pipeline model for inference')

            # Load trained pipeline model
            pipeline_model = PipelineModel.load(self.config.trans_pipeline_model_path)

            logging.info('Transforming new data')
            transformed_data = pipeline_model.transform(new_data)

            # Drop unnecessary features
            features_to_drop = ['user_id', 'product_id', 'category_id', 'brand', 'user_session',
                                '1_most_purchased_category', '2_most_purchased_category', '3_most_purchased_category',
                                "unscaled_features", 'RFM_scaled_features']

            transformed_data = transformed_data.drop(*features_to_drop)

            logging.info(f"Transformed data schema: {transformed_data.printSchema()} \n")
            
            return transformed_data

        except Exception as e:
            logging.error(f'Error during inference transformation: {e}')
            return None
