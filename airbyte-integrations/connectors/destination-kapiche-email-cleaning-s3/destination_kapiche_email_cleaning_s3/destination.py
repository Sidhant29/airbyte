#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import os
import json
from pathlib import Path
from contextlib import contextmanager
import logging
from typing import Any, Iterable, Mapping
from itertools import islice
from datetime import datetime

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import litellm
import polars as pl

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)

logger = logging.getLogger("airbyte")


@contextmanager
def set_google_credentials(credentials: str):
    """
    Create a json file from the credentials provided, and set the
    path in the environment variable GOOGLE_APPLICATION_CREDENTIALS.
    This file is how litellm will validate auth to google cloud.
    """
    google_creds = json.loads(credentials)
    creds_path = Path('./google_credentials.json')
    creds_path.touch()
    with open(creds_path, 'w') as f:
        json.dump(google_creds, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)
    os.environ['GRPC_DNS_RESOLVER'] = "native"
    try:
        yield
    finally:
        creds_path.unlink()


def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def get_file_name_from_pattern(pattern: str) -> str:
    """
    Returns the file name from specified pattern.
    Currently supported pattern operators:
        {date:yyyy_mm_dd},
        {date:yyyy_mm},
        {timestamp},
    """
    current_dt = datetime.now()
    date_yyyy_mm_dd = current_dt.strftime("%Y_%m_%d")
    date_yyyy_mm = current_dt.strftime("%Y_%m")
    timestamp = current_dt.timestamp()
    filename = pattern.format(
        date_yyyy_mm_dd=date_yyyy_mm_dd,
        date_yyyy_mm=date_yyyy_mm,
        timestamp=timestamp,
    )
    if not filename.endswith(".csv"):
        filename += ".csv"
    return filename


class DestinationKapicheEmailCleaningS3(Destination):

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage]
    ) -> AirbyteMessage:

        """
        TODO
        Reads the input stream of messages, config, and catalog to write data to
        the destination.

        This method returns an iterable (typically a generator of
        AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that
        every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault
        tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method
        as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration
            declared in spec.json
        :param configured_catalog: The Configured Catalog describing the
            schema of the data being received and how it should be persisted
            in the destination
        :param input_messages: The stream of input messages received from the
            source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage
            structs
        """

        streams = {s.stream.name for s in configured_catalog.streams}

        output = []
        text_field = config["data_field"]
        batch_size = 1000
        with set_google_credentials(config["google_credentials"]):
            for messages in batched(input_messages, batch_size):
                record_messages = []
                state_messages = []
                llm_messages = []
                for message in messages:
                    if message.type == Type.STATE:
                        # Emitting a state message means all records that came
                        # before it have already been published.
                        state_messages.append(message)
                        logger.info(message)
                    elif message.type == Type.RECORD:
                        if message.record.stream not in streams:
                            # Message contains record from a stream that is not
                            # in the catalog. Skip it!
                            continue
                        record_data = message.record.data
                        record_messages.append(record_data)
                        llm_message = [
                            {
                                'role': 'system',
                                'content': f"""
                                {config["model_prompt_system"]}
                                """
                            },
                            {
                                "role": "user",
                                "content": f"""
                                    {config["model_prompt_user"]}\n\n
                                    {record_data[text_field]}
                                """
                            }
                        ]
                        llm_messages.append(llm_message)
                try:
                    llm_responses = litellm.batch_completion(
                        model=config["model_name"],
                        messages=llm_messages,
                        max_tokens=1800,
                        temperature=0.3,
                        top_p=1.0,
                        timeout=30,
                        num_retries=3,
                    )
                    for response, record_data in zip(llm_responses, record_messages):
                        out = response.choices[0].message.content
                        output.append(
                            {
                                **record_data,
                                **json.loads(out),
                            }
                        )
                except Exception as e:
                    logger.error(f"LiteLLM issue: {llm_messages} {repr(e)}")
                    raise e
                yield from state_messages

        file_name = get_file_name_from_pattern(config["file_name_pattern"])
        pl.DataFrame(output).write_csv(file_name)
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config["aws_access_key_id"],
            aws_secret_access_key=config["aws_secret_access_key"],
            region_name=config["s3_bucket_region"],
        )
        bucket_name = config["s3_bucket_name"]

        with open(file_name, 'rb') as f:
            s3_client.upload_fileobj(f, bucket_name, file_name)

        Path(file_name).unlink()

    def check(
        self,
        logger: AirbyteLogger,
        config: Mapping[str, Any]
    ) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to
            the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and
            write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed
             to this logger)
        :param config: Json object containing the configuration of this
            destination, content of this json is as specified in
            the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            # Check if the data_fields are populated.
            if not config.get("data_field", None):
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message="No data fields provided"
                )

            # Check if the bucket exists
            s3_client = boto3.client(
                's3',
                aws_access_key_id=config["aws_access_key_id"],
                aws_secret_access_key=config["aws_secret_access_key"],
                region_name=config["s3_bucket_region"],
            )
            bucket_name = config.get("s3_bucket_name", None)
            if not bucket_name:
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message="No bucket name provided"
                )

            response = s3_client.head_bucket(Bucket=bucket_name)
            s3_metadata = response.get("ResponseMetadata", {})
            if not s3_metadata.get("HTTPStatusCode", None) == 200:
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message=f"Invalid response from s3: {response}"
                )
            
            # Set credentials for litellm

            # Check if we can receive messages
            #
            # NOTE: litellm.utils has a function called get_valid_moddels().
            # Ideally we'd use this to check if the model exists
            # and is reachable,
            # however, this function simply returns a list of models supported
            # by litellm, and checks which models it can reach via it's own
            # static list, based on what api keys have been provided.
            # Since vertex ai does not use explicit keys,
            # this function does not work for us at the moment.
            with set_google_credentials(config["google_credentials"]):
                litellm.completion(
                    model=config["model_name"],
                    messages=[{"role": "user", "content": "Return Something"}],
                    max_tokens=1,
                    num_retries=3,
                )
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)

        except (PartialCredentialsError, NoCredentialsError) as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"An S3 exception occurred: {repr(e)}"
            )

        except Exception as e:
            # Some other error occurred
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"An exception occurred: {repr(e)}"
            )
