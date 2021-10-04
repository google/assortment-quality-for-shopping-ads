# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging
import argparse
from typing import Any
from typing import Dict

from googleapiclient import discovery
from googleapiclient.http import build_http
from googleapiclient.errors import HttpError
from oauth2client import file
from oauth2client import tools
from oauth2client import client

DATA_STUDIO_TEMPLATE_ID = '53894476-b1df-4cd5-85c7-2636fc0e6025'

CREDENTIALS_STORAGE = 'credentials.dat'
CLIENT_SECRETS_FILE = 'client_secret.json'
SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/bigquery',
]

SQL_QUERIES = [
    'brand_coverage.sql',
    'category_coverage.sql',
    'product_coverage.sql',
    'product_price_competitiveness.sql'
]


MAX_RETRIES = 3

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class AssortmentQuality:
    def main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-p',
            '--project_id',
            required=True,
            type=str,
            help='a Google Cloud Platform Project ID')
        parser.add_argument(
            '-m',
            '--gmc_id',
            required=True,
            type=str,
            help='a Google Merchant Center ID')
        parser.add_argument(
            '-r',
            '--region',
            required=True,
            type=str,
            help='a Google Cloud Platform region name')
        parser.add_argument(
            '-d',
            '--dataset',
            required=True,
            type=str,
            help='an existing BigQuery dataset name')
        parser.add_argument(
            '-l',
            '--language',
            required=True,
            type=str,
            help='the language that will be used in the final template'
                 ' (ie. en-US)')
        parser.add_argument(
            '-c',
            '--country',
            required=True,
            type=str,
            help='the country on which the rankings will be calculated')
        parser.add_argument(
            '-e',
            '--expiration_time',
            required=True,
            type=int,
            help='number of days before the SQL table partitions expire')
        args = parser.parse_args()

        project_id = args.project_id
        gmc_id = args.gmc_id
        region_name = args.region
        dataset_name = args.dataset
        language = args.language
        country = args.country
        expiration_time = args.expiration_time

        self.authenticate()
        self.create_merchant_center_data_transfer(project_id, gmc_id, region_name, dataset_name, expiration_time)
        self.check_existing_custom_data_transfers(project_id, gmc_id, region_name, dataset_name, language, country)

    def authenticate(self):
        """
        Handles authentication to BiqQuery and BigQuery Data Transfer Service
        """
        client_secrets = os.path.join(
            os.path.dirname(__file__), CLIENT_SECRETS_FILE)

        flow = client.flow_from_clientsecrets(
            client_secrets,
            SCOPES,
            message=tools.message_if_missing(client_secrets))

        storage = file.Storage(CREDENTIALS_STORAGE)
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            credential_flags = argparse.Namespace(
                noauth_local_webserver=True,
                logging_level=logging.getLevelName(logger.getEffectiveLevel()))
            credentials = tools.run_flow(flow, storage, flags=credential_flags)

        http = credentials.authorize(http=build_http())

        self.bqdt_service = discovery.build('bigquerydatatransfer', 'v1', http=http)
        self.su_service = discovery.build('serviceusage','v1', http=http)
        self.bq_service = discovery.build('bigquery', 'v2', http=http)

    def create_merchant_center_data_transfer(self, project_id, gmc_id, region_name, dataset_name, expiration_time):
        """
        Creates (or reuses) a Merchant Center Data Transfer into BigQuery

        Args:
            project_id: The GCP Project ID where the Data Transfer will be created.
            gmc_id: The Google Merchant Center ID from which we will pull out data.
            region_name: The region name used throughout the process to locate
                where the data transfer happens.
            dataset_name: The name of the dataset where the output tables will be stored.
        """
        has_merchant_config = False
        transfer_display_name = f'Merchant Center Data Transfer for merchant {gmc_id}'
        project_location = f'projects/{project_id}/locations/{region_name}'
        transfer_configs = (self.bqdt_service.projects()
                            .locations()
                            .transferConfigs()
                            .list(parent=project_location)
                            .execute(num_retries=MAX_RETRIES))

        data = transfer_configs.get('transferConfigs') or []

        for tc in data:
            if tc['dataSourceId'] == 'merchant_center' and tc['displayName'] == transfer_display_name:
                has_merchant_config = True
                logger.info('There is an existing Merchant Center'
                            ' data transfer for the same Merchant ID. '
                            'If you want to replace it, please delete'
                            ' it from the UI and re-run this script.')

        if has_merchant_config:
            logger.info('Merchant Center config already exists. '
                        'Skipping Merchant Center config creation.')
        else:
            project_resource_name = f'projects/{project_id}'
            data_sources = (self.bqdt_service.projects()
                            .dataSources()
                            .list(parent=project_resource_name)
                            .execute())

            source = self.extract_merchant_center_data_source(data_sources)

            if source is None:
                logger.error('Data Source "merchant_center" not found')
            else:
                source_location = f'projects/{project_id}/locations/{region_name}/dataSources/merchant_center'
                valid_creds = (self.bqdt_service.projects()
                               .locations()
                               .dataSources()
                               .checkValidCreds(name=source_location)
                               .execute())
                logger.info(f'Valid Credentials found ? {valid_creds.get("hasValidCreds")}')

            self.check_or_create_dataset(project_id, dataset_name, region_name, expiration_time)

            body = {
                'name': f'projects/{project_id}/locations/{region_name}/transferConfigs/',
                'displayName': transfer_display_name,
                'dataSourceId': 'merchant_center',
                'schedule': 'every 72 hours',
                'disabled': 'false',
                'destinationDatasetId': f'{dataset_name}',
                'params': {
                    'merchant_id': f'{gmc_id}',
                    'export_products': True,
                    'export_price_benchmarks': True,
                    'export_best_sellers': True
                }
            }
            project_location = f'projects/{project_id}/locations/{region_name}'
            dt_response = self.bqdt_service.projects() \
                .locations() \
                .transferConfigs() \
                .create(parent=project_location, body=body)
            try:
                logger.info('Creating new BigQuery Data Transfer')
                dt_response.execute()
            except HttpError as err:
                logger.error(f'Please check that your BigQuery dataset already'
                             f' exists and that your Project Id and Region Name'
                             f' are correctly typed.\nError was :\n {err}')

    def extract_merchant_center_data_source(self, data_sources):
        """
        Searches for a Merchant Center source from a Data Source list.

        Args:
            data_sources: A list of Data Sources that we got from the BigQuery
                Data Transfer Service.

        Returns:
            The Merchant Center source (or None, if none was found)
        """
        for source in data_sources:
            for s in data_sources.get(source):
                if s.get('dataSourceId') == 'merchant_center':
                    return s

    def extract_dataset_from_list(self, datasets, project_id, dataset_name):
        """
        Checks if there is a project_id and dataset that exists from a list of
        datasets received from BigQuery.

        Args:
            datasets: A list of datasets received from BigQuery.
            project_id: A GCP project ID
            dataset_name: The name of a BigQuery dataset
        """
        for source in datasets.get('datasets'):
            if source.get('id') == f"{project_id}:{dataset_name}":
                return source

    def check_or_create_dataset(self, project_id, dataset_name, region_name, expiration_time):
        """
        Checks if a provided datasets exists, and creates it if not.

        Args :
            project_id: The GCP project ID where we will query of create
                the dataset.
            dataset_name: The name of the BigQuery dataset that needs to be
                returned or created.
            region_name: The region where the dataset should be located.

        Returns :
            dataset : The existing or created dataset.
        """
        # Check if the dataset already exists.
        datasets = (self.bq_service
                    .datasets()
                    .list(projectId=project_id)
                    .execute(num_retries=MAX_RETRIES))

        dataset = self.extract_dataset_from_list(datasets, project_id, dataset_name)

        # If it doesn't, we create it.
        if dataset is None:
            PARTITION_EXPIRATION = expiration_time * 24 * 60 * 60 * 1000  # 7 days in milliseconds
            body = {
                'datasetReference':
                    {'projectId': project_id,
                     'datasetId': dataset_name},
                'location': region_name,
                'defaultPartitionExpirationMs': PARTITION_EXPIRATION
            }

            dataset = (self.bq_service
                       .datasets()
                       .insert(projectId=project_id, body=body)
                       .execute(num_retries=MAX_RETRIES))

            logger.info("Created dataset {}.{}".format(project_id, dataset_name))
        else:
            logger.info("Dataset already exists, skipping creation.")

        return dataset

    def configure_sql(self, sql_path: str, query_params: Dict[str, Any]) -> str:
        """Configures parameters of SQL script with variables supplied.

        Args:
            sql_path: Path to SQL script.
            query_params: Configuration containing query parameter values.

        Returns:
            sql_script: String representation of SQL script with parameters
                assigned.
        """
        sql_script = self.read_file(sql_path)

        params = {}
        for param_key, param_value in query_params.items():
            # If given value is list of strings (ex. 'a,b,c'), create tuple of
            # strings (ex. ('a', 'b', 'c')) to pass to SQL IN operator.
            if isinstance(param_value, str) and ',' in param_value:
                params[param_key] = tuple(param_value.split(','))
            else:
                params[param_key] = param_value

        return sql_script.format(**params)

    def read_file(self, file_path: str) -> str:
        """Reads and returns contents of the file.
        Args:
            file_path: File path.
        Returns:
            content: File content.
        Raises:
            FileNotFoundError: If the provided file is not found.
        """
        try:
            with open(file_path, 'r') as stream:
                content = stream.read()
        except FileNotFoundError:
            raise FileNotFoundError(f'The file "{file_path}" could not be found.')
        else:
            return content

    def check_existing_custom_data_transfers(self, project_id, gmc_id, region_name, dataset_name, language, country):
        """Creates Custom Data Transfers from the provided set of SQL scripts.
       Args:
           project_id: The GCP project ID where the view will be created.
           gmc_id: The Google Merchant Center ID from which we will pull data
               to create the views.
           region_name : The region name used throughout the process to locate
                where the data transfer happens.
           dataset_name: The name of the BigQuery dataset where the views will
               be created.
       """

        params_replace = {
            'projectId': project_id,
            'gmcId': gmc_id,
            'datasetId': dataset_name,
            'language': language,
            'country' : country
        }

        for job in SQL_QUERIES:
            job_name = job.split('.')[0]
            scheduled_query_name = f'{job_name} for merchant {gmc_id} - ' \
                                   f'language {language} - country {country}'
            query_view = self.configure_sql(os.path.join('sql', job), params_replace)
            project_location = f'projects/{project_id}/locations/{region_name}'
            scheduled_query_already_exists = False

            transfer_config_list = (self.bqdt_service.projects()
                                    .locations()
                                    .transferConfigs()
                                    .list(parent=project_location)
                                    .execute(num_retries=MAX_RETRIES))

            transfer_configs = transfer_config_list.get('transferConfigs') or []

            for tc in transfer_configs:
                if tc['dataSourceId'] == 'scheduled_query' and tc['displayName'] == scheduled_query_name:
                    scheduled_query_already_exists = True
                    logger.info('There is an existing Scheduled Query called:'
                                f' {scheduled_query_name}.\n'
                                '  If you want to replace it, please delete'
                                ' it from the UI and re-run this script.\n')

            if not scheduled_query_already_exists:
                self.create_scheduled_query(project_id, region_name, job_name, dataset_name, query_view, project_location)

    def create_scheduled_query(self, project_id, region_name, job_name, dataset_name, query_view, project_location):
        body = {
            'name': f'projects/{project_id}/locations/{region_name}/transferConfigs/',
            'displayName': f'Scheduled query : {job_name}',
            'dataSourceId': 'scheduled_query',
            'schedule': 'every 24 hours',
            'disabled': 'false',
            'destinationDatasetId': f'{dataset_name}',
            'params': {
                'query': query_view,
                'destination_table_name_template': job_name,
                'write_disposition': 'WRITE_TRUNCATE',
                'partitioning_field': '',
            }
        }

        dt_response = self.bqdt_service.projects() \
            .locations() \
            .transferConfigs() \
            .create(parent=project_location, body=body)

        try:
            logger.info(f'Creating new Scheduled Query : {job_name}')
            dt_response.execute()
        except HttpError as err:
            logger.error(f'Please check that your BigQuery dataset already'
                         f' exists and that your Project Id and Region Name'
                         f' are correctly typed. If this is the first time '
                         f'you create a Scheduled Query on this project, '
                         f'try to create a dummy one from the UI (this '
                         f'should trigger a OAuth consent screen), then '
                         f'everything will work fine. '
                         f'\nError was :\n {err}')


if __name__ == '__main__':
    logger.info('Launching Assortment Quality ...')
    assortmentQuality = AssortmentQuality()
    assortmentQuality.main()
