#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_spotify import DestinationSpotify

if __name__ == "__main__":
    DestinationSpotify().run(sys.argv[1:])
