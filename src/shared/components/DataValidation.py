from src.config.configuration import ConfigurationManager
from src.logger import logging
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.exceptions import GreatExpectationsError
from src.utils.commons import save_json
from pathlib import Path
from src.entity.config_entity import DataValidationConfig


class DataValidation:

    def __init__(self, config=DataValidationConfig):

        self.config = config
        self.context = gx.get_context(mode='file')
        

    def validation_setup(self):
        try:
            logging.info('setting up validation')

            # Check if the data source already exists
            try:
                data_source = self.context.data_sources.get(self.config.data_source_name)
                logging.info(f'Data source "{self.config.data_source_name}" already exists.')
            except KeyError:
                # If the data source doesn't exist, create it
                data_source = self.context.data_sources.add_spark_filesystem(
                    name=self.config.data_source_name, base_directory=self.config.source_folder
                )
                logging.info(f'Data source "{self.config.data_source_name}" added.')

            # Check if the asset exists or create it
            try:
                data_asset = data_source.get_asset(name=self.config.asset_name)
                logging.info(f'Asset "{self.config.asset_name}" already exists.')
            except LookupError:
                # If asset doesn't exist, create it
                data_asset = data_source.add_directory_csv_asset(
                    name=self.config.asset_name, data_directory=self.config.data_directory, header=True
                )
                logging.info(f'Asset "{self.config.asset_name}" added.')

            # Add or get the batch definition
            try:
                batch_definition = data_asset.get_batch_definition(self.config.batch_definition_name)
                logging.info(f'Data source "{self.config.batch_definition_name}" already exists.')
            except KeyError:
                    batch_definition = data_asset.add_batch_definition_whole_directory(
                        self.config.batch_definition_name
                    )
                    logging.info(f'Batch definition "{self.config.batch_definition_name}" added.')

        except (Exception, GreatExpectationsError) as e:
            logging.error(f'Error occurred: {e}')
            # Fetch existing batch definition if there's an error
            data_source = self.context.data_sources.get(self.config.data_source_name)
            data_asset = data_source.get_asset(name=self.config.asset_name)
            batch_definition = data_asset.get_batch_definition(self.config.batch_definition_name)

        # Return the batch definition
        return batch_definition

    def expectation_suite(self):
        try:
            # Check if the expectation suite already exists
            try:
                expectation_suite = self.context.suites.get(self.config.expectation_suite_name)
                logging.info(f'Expectation suite "{self.config.expectation_suite_name}" already exists.')
            except Exception:
                # If the expectation suite doesn't exist, create it
                expectation_suite = self.context.suites.add(
                    gx.ExpectationSuite(name=self.config.expectation_suite_name)
                )
                logging.info(f'Expectation suite "{self.config.expectation_suite_name}" created.')
        except:
            # If an error occurs (e.g., the suite exists but could not be fetched), delete and recreate
            self.context.suites.delete(name=self.config.expectation_suite_name)
            expectation_suite = self.context.suites.add(
                gx.ExpectationSuite(name=self.config.expectation_suite_name)
            )
            logging.info(f'Expectation suite "{self.config.expectation_suite_name}" recreated.')

        # Define the expectations
        expectations = [
            *[gxe.ExpectColumnToExist(column=x, column_index=0) for x in ['price','product_id','user_id','category_id', 'event_time',
                                                                          'event_type','category_code', 'brand','user_session']],

            gxe.ExpectColumnDistinctValuesToBeInSet(column='event_type', value_set=['cart','remove_from_cart','purchase','view']),

            gxe.ExpectCompoundColumnsToBeUnique(column_list=['price','product_id','user_id','category_id', 'event_time','event_type',
                                                             'category_code', 'brand','user_session']),

            *[gxe.ExpectColumnValuesToNotBeNull(column=x) for x in ['user_id','event_time','product_id','category_id','event_type','user_session']],

            *[gxe.ExpectColumnValuesToBeOfType(column=x, type_='StringType') for x in ['user_id','product_id','category_id','user_session','event_type']],

            gxe.ExpectColumnValuesToBeInTypeList(column='price', type_list=['DoubleType','FloatType','IntegerType']),
            gxe.ExpectColumnValuesToBeOfType(column='event_time', type_='TimestampType')
        ]

        # Add expectations to the suite
        for expectation in expectations:
            expectation_suite.add_expectation(expectation)

        return expectation_suite

    def validate(self):
        batch_definition = self.validation_setup()
        # Validate Batch using Expectation Suite.
        expectation_suite = self.expectation_suite()

        try:
            # Check if validation definition already exists
            validation_definition = self.context.validation_definitions.get(self.config.validation_definition_name)
            logging.info(f'Validation definition "{self.config.validation_definition_name}" already exists.')
        except Exception:
            # If the validation definition doesn't exist, create it
            validation_definition = gx.ValidationDefinition(
                data=batch_definition, suite=expectation_suite, name=self.config.validation_definition_name
            )
            logging.info(f'Validation definition "{self.config.validation_definition_name}" created.')

        validation_results = validation_definition.run().to_json_dict()

        save_json(filepath=Path(self.config.status_file),data=validation_results)