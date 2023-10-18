from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from urllib.parse import urljoin, urlencode
import os
import requests
import json
import logging
import io
from datetime import datetime
from typing import (
    Dict,
    Optional,
    Any
)

class ApiCallOperator(BaseOperator):
    """
    Custom Airflow operator to make an API call, retrieve data, and upload it to Google Cloud Storage.

    :param endpoint: The API endpoint URL.
    :type endpoint: str
    :param destination_bucket_name: The name of the Google Cloud Storage bucket where data will be uploaded.
    :type destination_bucket_name: str
    :param method: The HTTP method for the API call (e.g., 'GET' or 'POST').
    :type method: str
    :param http_conn_id: The Airflow connection ID for the HTTP connection.
    :type http_conn_id: str
    :param params: Optional parameters to include in the API call.
    :type params: dict
    :param kwargs: Other keyword arguments passed to the BaseOperator.
    :type kwargs: Any
    """

    def __init__(
            self,
            endpoint: str,
            destination_bucket_name: str,
            method: str = 'GET',
            http_conn_id: str = 'http_default',
            params: Optional[Dict[str, Any]] ={},
            **kwargs,
            
    ):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.method = method
        self.destination_bucket_name = destination_bucket_name
        self.http_conn_id = http_conn_id
        self.params = params
        
        
          

    def execute(self, context):
        """
        Execute the API call, retrieve data, and upload it to Google Cloud Storage.

        :param context: The task execution context.
        :type context: dict
        """
        http_hook = HttpHook(method=self.method, http_conn_id=self.http_conn_id)

        gcs_hook = GCSHook()

        # Log the base URL and endpoint
        base_url = http_hook.base_url
        logger = logging.getLogger(__name__)
        logger.info("Base URL: %s", base_url)
        logger.info("Endpoint: %s", self.endpoint)
        logger.info("Full URL: %s", base_url + self.endpoint)
        logger.info("Parameters: %s", self.params)

        # Construct the full URL with the query parameters
        full_url = urljoin(base_url, self.endpoint) + '?' + urlencode(self.params)
        logger.info("Full URL: %s", full_url)

        response = http_hook.run(endpoint=full_url, extra_options={"check_response": False})
        
        # Check response status
        if response.status_code != 200:
            raise ValueError(f"API call failed with status {response.status_code}: {response.content}")
        
        raw_data = response.json()

        # Check if raw_data is empty and raise an error
        if not raw_data:
            raise ValueError("API call returned empty data")
        
         # Log the value of raw_data
        logger = logging.getLogger(__name__)
        logger.info("Raw data: %s", raw_data)

        # Create an in-memory buffer
        buffer = io.StringIO()

        # Write each item in the raw_data dictionary as a JSON string followed by a newline character
        for item in raw_data:
            buffer.write(json.dumps(item))
            buffer.write('\n')
            
        # Seek the buffer to the beginning
        buffer.seek(0)
        
        gcs_hook.upload(
            bucket_name=self.destination_bucket_name,
            object_name= f'test_data_json_{datetime.now().strftime("%Y%m%d")}',
            data=buffer.getvalue(),
            mime_type="application/json",
            timeout=120
        )
            
        

