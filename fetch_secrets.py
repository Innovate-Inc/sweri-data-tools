import logging
import sys

from sweri_utils.s3 import fetch_secrets

if __name__ == "__main__":
    if len(sys.argv) < 5:
        raise ValueError("Please provide the secret name and output file name for both secrets.")

    fetch_secrets(secret_name=sys.argv[1], out_file=sys.argv[2])
    fetch_secrets(secret_name=sys.argv[3], out_file=sys.argv[4])
    logging.info(f"Fetched secrets and saved them to {sys.argv[2]} and {sys.argv[4]}.")