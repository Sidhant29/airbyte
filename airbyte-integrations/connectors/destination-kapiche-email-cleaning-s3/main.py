#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys
from pathlib import Path

from destination_kapiche_email_cleaning_s3 import DestinationKapicheEmailCleaningS3

if __name__ == "__main__":
    DestinationKapicheEmailCleaningS3().run(sys.argv[1:])
