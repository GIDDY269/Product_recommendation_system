from src.entity.config_entity import DataTransformationConfig
from src.utils.commons import spark_session,create_directories
from src.logger import logging
import findspark
findspark.init()
 
from pyspark.sql import types,Window

from pyspark.sql.types import StructField,StructType,FloatType
from pyspark.ml.feature import FeatureHasher, VectorAssembler,StringIndexer,StandardScaler
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans

import os
from glob import glob
import math






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
                df.write.parquet(parquet_path,mode='overwrite')

                # Return the path to the newly saved Parquet file
                return parquet_path

        except Exception as e:
            logging.error(f'Could not load data: {e}')
            return None
        
    def train_val_test_split(self, data_path):
        logging.info('splitting data into training, validation and test split')
        try:
            """
            Splits the data into train, validation, and test sets based on user interactions and event time.

            Parameters:
            data (DataFrame): Input DataFrame with columns 'user_id', 'event_time', and other features.

            """

            data = self.spark.read.parquet(data_path)

            # remove remove from cart eventy type
            data = data.filter(data['event_type'] != 'remove_from_cart')

            #  Compute total interactions for each user
            user_total_interactions = data.groupBy("user_id").agg(F.count("*").alias("total_interactions"))

            #  Join the total interactions back to the original DataFrame
            data = data.join(user_total_interactions, on="user_id")

            # filter out interactions with less than 8 interactions
            data = data.filter(F.col('total_interactions') > 8)
            logging.info('filter out users with low interactions')

            # Assign interaction ranks within each user based on event_time
            user_window = Window.partitionBy("user_id").orderBy("event_time")
            df_with_rank = data.withColumn("interaction_rank", F.row_number().over(user_window))

            # Define split cutoffs
            train_cutoff = 0.7  # 70% for training
            val_cutoff = 0.85   # Next 15% for validation (leaving 10% for testing)

        
            df_with_rank = df_with_rank.withColumn(
                "split",
                F.when(
                    (df_with_rank["interaction_rank"] <= (df_with_rank["total_interactions"] * train_cutoff)) |
                    (df_with_rank["total_interactions"] <= 3),  # Include users with <= 2 interactions in training
                    "train"
                ).when(
                    (df_with_rank["interaction_rank"] > (df_with_rank["total_interactions"] * train_cutoff)) &
                    (df_with_rank["interaction_rank"] <= (df_with_rank["total_interactions"] * val_cutoff)),
                    "val"
                ).otherwise("test")
            )


            # Filter data into train, validation, and test DataFrames
            train_df = df_with_rank.filter("split = 'train'").drop("split",'total_interactions')
            val_df = df_with_rank.filter("split = 'val'").drop("split",'total_interactions')
            test_df = df_with_rank.filter("split = 'test'").drop("split",'total_interactions')
            logging.info('Data splitted into train,val nad test set')

            # save set into feature store
            train_path = self.config.train_datapath
            val_path = self.config.val_datapath
            test_path = self.config.test_datapath
            train_df.write.parquet(self.config.train_datapath,mode='overwrite')
            val_df.write.parquet(self.config.val_datapath,mode='overwrite')
            test_df.write.parquet(self.config.test_datapath,mode='overwrite')
            
            logging.info(f"Returning paths: {train_path}, {val_path}, {test_path}")
            return train_path,val_path,test_path

        except Exception as e :
            logging.error(f'Could not split data : {e}')

    def feature_engineering(self,data_path):

        try: 

            logging.info(f'Starting feature engineering ofr {data_path}')
            

            data = self.spark.read.parquet(data_path) # read data

            # handling the missing values
            data = data.drop('category_code')
            data = data.fillna({'brand':'UNKNOWN'})

            # Add the 'is_brand_missing' column to indicate if the brand is missing
            data = data.withColumn('is_brand_missing', F.when(F.col('brand') == 'UNKNOWN',F.lit(1)).otherwise(F.lit(0)))
            data = data.dropna(subset=['user_session'])

            # dropduplicates 
            data = data.dropDuplicates()

            data = data.withColumn('price',F.abs(data['price']))

            cart_df = data.filter(F.col('event_type') == 'cart')
            purchase_df = data.filter(data['event_type'] == 'purchase')
            view_df = data.filter(F.col('event_type') == 'view')

            # purchases made in the last 2 days
            logging.info('creating features : two_day_purchase')
            purchase_df_per_2day = purchase_df.withColumn('two_day_purchase', 
                                                                        F.from_unixtime(
                                                                            F.floor(F.unix_timestamp(F.col('event_time')) / (2 * 86400))* (2 * 86400) 
                                                                            )
                                                                        )
            purchase_df_per_2day = purchase_df_per_2day.groupBy('user_id','two_day_purchase').agg(F.count('*').alias('Num_purchase_per_2day'))
            purchase_df_per_2day = purchase_df_per_2day.withColumnRenamed("user_id", "train_user_id") # rename 'user_id to prevent ambitiuity
            # Rename user_id in purchase_df_per_2day to avoid duplication in final result
            purchase_df_per_2day = purchase_df_per_2day.withColumnRenamed("user_id", "train_user_id")

            data = data.join(
                purchase_df_per_2day,
                (data['user_id'] == purchase_df_per_2day['train_user_id']) & 
                (F.floor(F.unix_timestamp(data['event_time']) / (2*86400)) ==  F.floor(F.unix_timestamp(purchase_df_per_2day['two_day_purchase']) / (2 * 86400))),
                how='left'
            ).drop('two_day_purchase','train_user_id').na.fill({'Num_purchase_per_2day' : 0 })

            logging.info('combined feature(two_day_purchase) to main dataframe')

            # creating RFM FEATURES
            # recency
            logging.info('creating feature : recency')
            reference_time = data.select(F.max(F.col('event_time')).cast('long')).collect()[0][0]
            last_purchase_time = purchase_df.groupBy('user_id').agg(F.max('event_time').alias('last_purchase_time'))
            recency_df = last_purchase_time.withColumn('Recency', (F.lit(reference_time) - F.col('last_purchase_time').cast('long'))/86400).drop('last_purchase_time') # calculate rency and convert to days

            data = data.join(
                recency_df ,
                on = 'user_id',
                how = 'left'
            )
            # fill missing values
            min_event_time = data.select(F.min('event_time')).collect()[0][0]
            max_recency_value = (reference_time - min_event_time.timestamp()) / 86400  # Convert to days
            # Fill missing Recency values with the calculated scalar max_recency_value
            data = data.na.fill({'Recency': max_recency_value})

            # frequency
            logging.info('creating feature : frequency')
            frequency_df = purchase_df.groupBy('user_id').agg(F.count(F.col('event_type')).alias('Frequency'))

            data = data.join(
                frequency_df,
                on='user_id',
                how='left'
            ).na.fill({'Frequency':0})

            logging.info('creating feature : monetary value')
            monetary_value = purchase_df.groupBy('user_id') \
                        .agg(F.sum(F.col('price')).alias('Monetary'))

            data = data.join(
                monetary_value,
                on='user_id',
                how='left'
            ).na.fill({'Monetary':0})


            # Total number of purchases by a user
            logging.info('combined feature : total number purchases to main dataframe')
            total_purchases_by_user = purchase_df.groupBy('user_id').agg(F.count('event_type').alias('total_purchases_by_user'))
            data = data.join(
                total_purchases_by_user,
                on='user_id',
                how ='left'
            ).na.fill({'total_purchases_by_user':0})

            #total number of views by user
            logging.info('combined feature: total_views_by _user to main dataframe')
            total_views_by_user = view_df.groupBy('user_id').agg(F.count('event_type').alias('total_views_by_user'))
            data = data.join(
                total_views_by_user,
                on = 'user_id',
                how = 'left'
            ).na.fill({'total_views_by_user':0})

            # total number of add to cart by user
            logging.info('combined feature: total_add_to cart to main dataframe')
            total_add_cart = cart_df.groupBy('user_id').agg(F.count('event_type').alias('total_add_to_cart_by_user'))
            data = data.join(
                total_add_cart,
                on = 'user_id',
                how = 'left'
            ).na.fill({'total_add_to_cart_by_user':0})

            # Total number of products purchased
            logging.info('combined feature; total number of products purchased to main dataframe')
            total_purchases_by_user_across_items = purchase_df.groupBy('user_id').agg(
                F.countDistinct('product_id').alias('total_purchases_by_user_across_items'))
            data = data.join(
                total_purchases_by_user_across_items,
                on='user_id',
                how='left'
            ).na.fill({'total_purchases_by_user_across_items':0})

            # total number of product add to cart
            logging.info('combined feature: total number of products add to cart to main dataframe')
            total_add_cart_by_user_across_items = cart_df.groupBy('user_id').agg(
                F.countDistinct('product_id').alias('total_add_cart_by_user_across_items')
            )
            data = data.join(
                total_add_cart_by_user_across_items,
                on = 'user_id',
                how = 'left',
            ).na.fill({'total_add_cart_by_user_across_items':0})

            # Time spent between view and purchase
            logging.info('combined feature: Time spent between view and purchase to main dataframe')
            view_time = view_df.groupBy('user_id','product_id').agg(F.min('event_time').alias('view_time'))
            purchase_time = purchase_df.groupBy('user_id','product_id').agg(F.min('event_time').alias('purchase_time'))
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


            # average purchase price 
            logging.info('creating feature : Avg_purchase_price')
            avg_purchase_price = purchase_df.groupBy('user_id').agg(F.format_number(F.mean('price'),2).cast(FloatType()).alias('Avg_purchase_price'))
            avg_purchase_price = avg_purchase_price.withColumnRenamed("user_id", "train_user_id")
            data = data.join(
                avg_purchase_price,
                data['user_id'] == avg_purchase_price['train_user_id'],
                how='left'
            ).drop('train_user_id').na.fill({'Avg_purchase_price' : 0})
            logging.info('combined feature(Avg__purchase_price) to main dataframe')


            # normalized price
            logging.info('creating feature : normalised_product_price')
            window_cat = Window.partitionBy('category_id')
            data = data.withColumn('avg_cat_price',F.avg('price').over(window_cat))
            data = data.withColumn('normalised_price',F.col('price')/F.col('avg_cat_price')).drop('avg_cat_price')



            # most purchase category of user 
            # groupby user id and category and create a window specification to get the top three brands
            logging.info('creating most purchase category')
            category_count = purchase_df.groupBy('user_id','category_id').agg(F.count('category_id').alias('category_count'))
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
            
            logging.info('combined feature(most _purchase_category) to main dataframe')


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
            session_view = data.filter(F.col('event_type') == 'view')
            num_session_view = session_view.groupBy('user_id','user_session').agg(F.count('event_type').alias('num_session_views'))
            # combine dataframe
            data = data.join(
                num_session_view,
                on = ['user_id','user_session'],
                how='left'
            ).na.fill({'num_session_views' : 0})

            # number of product added to cart in a sesiion
            logging.info('creating feature : num_session_cart')
            session_cart = data.filter(F.col('event_type') == 'cart')
            num_session_cart = session_cart.groupBy('user_id','user_session').agg(F.count('event_type').alias('num_session_cart'))
            # combine dataframe
            data = data.join(
                num_session_cart,
                on = ['user_id','user_session'],
                how='left'
            ).na.fill({'num_session_cart' : 0})

            # session purchase
            logging.info('creating feature : num_session_purchase')
            session_purchase = data.filter(F.col('event_type') == 'purchase')
            num_session_purchase = session_purchase.groupBy('user_id', 'user_session').agg(F.count('event_type').alias('num_session_purchase'))
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

            # total product purchased
            logging.info('creating feature : total_num_product_purchased')
            product_purchase = purchase_df.groupBy('product_id').agg(
                F.count('product_id').alias('total_num_product_purchased')
            )
            data = data.join(
                product_purchase,
                on=['product_id'],
                how='left'
            ).na.fill({'total_num_product_purchased':0})

            # total number of product views
            logging.info('creating feature : total_product_views')
            view_df = data.filter(F.col('event_type') == 'view')
            # groupby by product
            product_views = view_df.groupBy('product_id').agg(F.count('product_id').alias('total_product_views'))
            data = data.join(
                product_views,
                on = ['product_id'],
                how='left'
            ).na.fill({'total_product_views':0})

            #popularity percentile
            # Compute max view_count per category
            logging.info('createing feature : popularity percentile')
            data = data.withColumn(
                "max_view_category",
                F.max("total_product_views").over(window_cat)
            )
            # Compute popularity percentile
            data = data.withColumn(
                "popularity_percentile",
                F.when(F.col("max_view_category") > 0, F.col("total_product_views") / F.col("max_view_category")).otherwise(0)
            ).drop("max_view_category")

            # total product add to cart
            logging.info('creating feature : total_product_cart')
            product_cart = cart_df.groupBy('product_id').agg(F.count('product_id').alias('total_product_cart'))
            data = data.join(
                product_cart,
                on=['product_id'],
                how='left'
            ).na.fill({'total_product_cart':0})

            # product cart  purchase conversion rate 
            logging.info('creating feature :  product cart_purchase_conversion rate')
            data = data.withColumn('product_cart_to_purchase_conversion_ratio', F.when(F.col('total_num_product_purchased') == 0 , 0).otherwise(
                F.col('total_product_cart') / F.col('total_num_product_purchased')
            ))
            data = data.withColumn('product_cart_to_purchase_conversion_ratio', F.round(F.col('product_cart_to_purchase_conversion_ratio'), 2))

            # conversion rate from view to purchase
            logging.info('creating feature :  view to purchase_conversion rate')
            data = data.withColumn('view_to_purchase_conversion_rate_for_item', F.when(F.col('total_product_views') == 0,0).otherwise(
                                                                         F.col('total_num_product_purchased')/F.col('total_product_views')))
            
            data = data.withColumn('view_to_purchase_conversion_rate_for_item', F.round(F.col('product_cart_to_purchase_conversion_ratio'), 2))

            # view_to_purchase_ratio
            logging.info('creating feature :  view_to_purchase ratio')
            data = data.withColumn('view_to_purchase_ratio', F.when(F.col('total_num_product_purchased') == 0 , 0).otherwise(
                                                     F.col('total_product_views')/F.col('total_num_product_purchased')))
            data = data.withColumn('view_to_purchase_ratio', F.round(F.col('view_to_purchase_ratio'), 2))



            #user_product_view_count
            logging.info('creating feature : user_product_view')
            user_product_view = view_df.groupBy('user_id','product_id').agg(F.count('event_type').alias('user_product_view'))
            data = data.join(
                user_product_view,
                on=['user_id','product_id'],
                how='left'
            ).na.fill({'user_product_view':0})

            #user_product_add to cart _count
            logging.info('creating feature : user_product_cart')
            user_product_cart = cart_df.groupBy('user_id','product_id').agg(F.count('event_type').alias('user_product_cart'))

            data = data.join(
                user_product_cart,
                on=['user_id','product_id'],
                how='left'
            ).na.fill({'user_product_cart':0})


            #user_product_add to cart _count
            logging.info('creating feature : user_product_purchase')
            user_product_purchase = purchase_df.groupBy('user_id','product_id').agg(F.count('event_type').alias('user_product_purchase'))
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
                                   F.col('user_product_purchase') * F.col('interaction_weight'))

            # Temporalfeatures
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

            data = data.orderBy('event_time').drop('hour','minutes','seconds','interaction_rank','interaction_weight','user_product_view','user_product_cart','user_product_purchase')
            #data.show()

            data.write.parquet(data_path,mode='overwrite')
            logging.info(f'saved featured data in {data_path}')
            

        except Exception as e :
            logging.error(f'could not complete feature engineering because : {e}')

    def feature_transformation(self, train_path, val_path, test_path):
        try:
            logging.info('Performing feature transformation')

            train_data = self.spark.read.parquet(train_path)
            val_data = self.spark.read.parquet(val_path)
            test_data = self.spark.read.parquet(test_path)

            # Define transformations
            features_to_hash = ['user_id', 'product_id', 'category_id', 'brand', 'user_session','1_most_purchased_category',
                                '2_most_purchased_category','3_most_purchased_category']
            
            hashingTFs = [FeatureHasher(inputCols=features_to_hash, outputCol="categorical_hash_values", numFeatures=8000)]
            user_id_indexer = StringIndexer(inputCol='user_id', outputCol='user_id_index',handleInvalid='keep')
            product_id_indexer = StringIndexer(inputCol='product_id', outputCol='product_id_index',handleInvalid='keep')
            assembler = VectorAssembler(inputCols=["Recency", "Frequency", "Monetary"], outputCol="unscaled_features")
            scaler = StandardScaler(inputCol="unscaled_features", outputCol="RFM_scaled_features", withStd=True, withMean=True)
            kmeans = KMeans(k=4, seed=1, featuresCol="RFM_scaled_features", predictionCol="RFM_user_cluster")



            pipeline = Pipeline(stages=[user_id_indexer, product_id_indexer] + hashingTFs + [assembler,scaler,kmeans])

            # Fit on train and transform all sets
            logging.info('fitting train data')
            pipeline_model = pipeline.fit(train_data)
            logging.info('transforming datasets')
            features_to_drop = features_to_hash + ["Recency", "Frequency", "Monetary", "unscaled_features"]
    
            train_data = pipeline_model.transform(train_data).drop(*features_to_drop)
            val_data = pipeline_model.transform(val_data).drop(*features_to_drop)
            test_data = pipeline_model.transform(test_data).drop(*features_to_drop)

            logging.info(f"Train data schema: {train_data.printSchema()} \n")
            logging.info(f"Validation data schema: {val_data.printSchema()}\n")
            logging.info(f"Test data schema: {test_data.printSchema()}\n")


            # Save transformed data
            logging.info('saving transformed datasets')
            logging.info(f'train path {self.config.train_transformed_datapath}')
            train_data.write.parquet(self.config.train_transformed_datapath,mode='overwrite')
            logging.info('saved train transformed data')
            val_data.write.parquet(self.config.val_transformed_datapath,mode='overwrite')
            logging.info('saved val transformed data')
            test_data.write.parquet(self.config.test_transformed_datapath,mode='overwrite')
            logging.info('saved test transformed data')

            logging.info('Feature transformation completed')
            self.spark.stop()

        except Exception as e:
            logging.error(f'Error during feature transformation: {e}')
