if __name__ == '__main__':

    from argparse import ArgumentParser
    import sys
    import os
    import shutil
    import subprocess
    from pathlib import Path
    import re
    from filelock import FileLock
    from datetime import datetime

    import datalad.api as dl
    import pandas as pd


    parser = ArgumentParser()

    parser.add_argument('--output_datasets', nargs='+', help='<Required> Set flag', required=True)


    args = parser.parse_args()

    output_datasets = args.output_datasets

    print(output_datasets)