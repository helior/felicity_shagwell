import boto3
from botocore.exceptions import NoCredentialsError
import json
import csv
from io import StringIO, BytesIO

# Connect to S3
s3 = boto3.client("s3")

# Get the file from S3
try:
    response = s3.get_object(Bucket="so0050-0673f74da1e9-648353486619-us-west-2-proxy", Key="rekognition_renamed_files/celebrity_brief3.json")
    raw_data = response["Body"].read().decode("utf-8")
    data = json.loads(raw_data)
except s3.exceptions.NoSuchKey:
    print("Error: JSON file not found")
    exit()
except json.decoder.JSONDecodeError:
    print("Error: JSON file is not properly formatted")
    exit()
except NoCredentialsError:
    with open('celeb_all_metadata_clean2.json') as celebrity_file:
        data = json.load(celebrity_file)
        

# Set the output file path
output_file = "celeb_movies_brief.csv"
headers = ['~id', '~from', '~to', '~label']

# Parse the JSON
rows = []
for element in data:
    if not isinstance(element, dict):
        print('Skipping malformed data, expected dictionary and got "{}"'.format(element))
        continue

    if not element.get('UUID') or not element.get('Movie'):
        print('Warning: Missing "UUID" or "Movie" attributes.')
        continue

    movie_row = {
        '~id': element.get('UUID'),
        '~label': element.get('Movie')
    }
    rows.append(movie_row)

    if element.get('Celebrities'):
        if not isinstance(element.get('Celebrities'), list):
            print('Skipping malformed data, expected list and got "{}"'.format(element))
            continue

        for celeb_entry in element.get('Celebrities'):
            celebrity = celeb_entry.get('Celebrity')
            if not isinstance(celebrity, dict):
                print('Skipping malformed data, expected dictionary and got "{}"'.format(element))
                continue
            celebrity_row = {
                '~id': '{}_celebrity_{}'.format(element.get('UUID'), celebrity.get('Id')),
                '~from': element.get('UUID'),
                '~to': celebrity.get('Id'),
                '~label': celebrity.get('Name')
            }
            rows.append(celebrity_row)

# Write CSV file
pseudo_file = StringIO()
writer = csv.DictWriter(pseudo_file, headers, delimiter="\t")
writer.writeheader()
writer.writerows(rows)

# Upload the file contents to S3
try:
    s3.upload_fileobj(pseudo_file, "so0050-0673f74da1e9-648353486619-us-west-2-proxy", output_file)
except NoCredentialsError:
    print('No credentials to upload :P')
    exit()
