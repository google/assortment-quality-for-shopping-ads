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

SQL_VIEWS = [
    'brand_coverage.sql',
    'category_coverage.sql',
    'product_coverage.sql'
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
        args = parser.parse_args()

        project_id = args.project_id
        gmc_id = args.gmc_id
        region_name = args.region
        dataset_name = args.dataset

        self.authenticate()
        self.create_merchant_center_data_transfer(project_id, gmc_id, region_name, dataset_name)
        self.create_custom_views(project_id, gmc_id, dataset_name)

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
                logging_level=logger.getLevelName(logger.getLogger().getEffectiveLevel()))
            credentials = tools.run_flow(flow, storage, flags=credential_flags)

        http = credentials.authorize(http=build_http())

        self.bqdt_service = discovery.build('bigquerydatatransfer', 'v1', http=http)
        self.bq_service = discovery.build('bigquery', 'v2', http=http)

    def create_merchant_center_data_transfer(self, project_id, gmc_id, region_name, dataset_name):
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
        project_location = f'projects/{project_id}/locations/eu'
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

                raise

            self.check_or_create_dataset(project_id, dataset_name, region_name)

            body = {
                'name': f'projects/{project_id}/locations/{region_name}/transferConfigs/',
                'displayName': transfer_display_name,
                'dataSourceId': 'merchant_center',
                'schedule': 'every 24 hours',
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
            logger.debug(data_sources.get(source))
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

    def check_or_create_dataset(self, project_id, dataset_name, region_name):
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
            body = {
                'datasetReference':
                    {'projectId': project_id,
                     'datasetId': dataset_name},
                'location': region_name
            }

            dataset = (self.bq_service
                       .datasets()
                       .insert(projectId=project_id, body=body)
                       .execute(num_retries=MAX_RETRIES))

            logger.info("Created dataset {}.{}".format(project_id, dataset_name))
        else:
            logger.info("Dataset already exists, skipping creation.")

        return dataset

    def create_custom_views(self, project_id, gmc_id, dataset_name):
        """Creates BigQuery views from the provided set of SQL scripts.

        Args:
            project_id: The GCP project ID where the view will be created.
            gmc_id: The Google Merchant Center ID from which we will pull data
                to create the views.
            dataset_name: The name of the BigQuery dataset where the views will
                be created.
        """
        params_replace = {
            'projectId': project_id,
            'gmcId': gmc_id,
            'datasetId': dataset_name
        }

        for view in SQL_VIEWS:
            query_view = self.configure_sql(os.path.join('sql', view), params_replace)
            try:
                (self.bq_service
                    .jobs()
                    .query(projectId=project_id,
                           body={
                               'query': query_view,
                               'useLegacySql': False
                           })
                    .execute(num_retries=MAX_RETRIES))
                logger.debug("View from {0} was created (or updated).".format(view))
            except HttpError:
                logger.error(f"Failed to create the view from {view}. Since some"
                             "tables need 90 minutes to be calculated after"
                             " data transfer creation, please wait (90 minutes)"
                             " before re-running this script")

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


if __name__ == '__main__':
    logger.info('Launching Assortment Quality ...')
    assortmentQuality = AssortmentQuality()
    assortmentQuality.main()
