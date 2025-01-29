train_cutoff = 0.7  # 70% for training
val_cutoff = 0.85  # Additional 15% for validation (70% + 15%)


df_with_rank = df_with_rank.withColumn(
    "is_train",
    F.when(
        (df_with_rank["interaction_rank"] <= (df_with_rank["total_interactions"] * train_cutoff)) | 
        (df_with_rank["total_interactions"] <= 1),  # Include customers with 1 interaction
        True
    ).otherwise(False)
)



train_df = df_with_rank.filter("is_train = true")
test_df = df_with_rank.filter("is_train = false")




    def read_schema(self):

        fields = []

        logging.info('Reading schema from schema.yaml')
        logging.info(f'schema {self.config.schema}')

        for  field_dict in self.config.schema['schema']['fields']:
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
                df.write.mode('overwrite').parquet(parquet_path)

                # Return the path to the newly saved Parquet file
                return parquet_path

        except Exception as e:
            logging.error(f'Could not load data: {e}')
            return None
        


## run redis

docker run --name redis -p 6379:6379 -d redis


   data = data.withColumn('interaction_weight' , F.when(
                    F.col('event_type') == 'view',0.3).when(
                        F.col('event_type') == 'cart' ,0.7
                    ).when(
                        F.col('event_type') == 'purchase',2
                    ).when(
                        F.col('event_type') == 'remove_from_cart',-0.3
                    )
                      ).drop('event_type')



# number of produt removed from cart

session_frm_cart = new_training_data.filter(F.col('event_type') == 'remove_from_cart')
num_session_frm_cart = session_frm_cart.groupBy('user_id','user_session').agg(F.count('event_type').alias('num_session_frm_cart'))

num_session_frm_cart.show()

# combine dataframe

new_training_data= new_training_data.join(
    num_session_frm_cart,
    on = ['user_id','user_session'],
    how='left'
).na.fill({'num_session_frm_cart' : 0})