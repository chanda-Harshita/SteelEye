import requests
import xml.etree.ElementTree as ET
import zipfile
import io
import csv
import boto3

# Step 1: Send GET request to retrieve the XML file list
url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
params = {
    "q": "*",
    "fq": "publication_date:[2021-01-17T00:00:00Z TO 2021-01-19T23:59:59Z]",
    "wt": "xml",
    "indent": "true",
    "start": "0",
    "rows": "100"
}
response = requests.get(url, params=params)
response.raise_for_status()

# Step 2: Parse the XML response
root = ET.fromstring(response.content)

# Step 3: Find the download link for DLTINS file type
download_link = None
for doc in root.findall("./result/doc"):
    file_type = doc.find("./str[@name='file_type']").text
    if file_type == "DLTINS":
        download_link = doc.find("./str[@name='download_link']").text
        break

if download_link is None:
    print("DLTINS file not found")
    exit()

# Step 4: Download the zip file
response = requests.get(download_link)
response.raise_for_status()

# Step 5: Extract the XML file from the zip file
with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
    file_name = zip_file.namelist()[0]  # assume only one file in the zip
    xml_content = zip_file.read(file_name)

# Step 6: Parse the XML file
root = ET.fromstring(xml_content)

# Step 7: Extract data fields and store in list of dictionaries
data = []
for instr in root.findall(".//FinInstrmGnlAttrbts"):
    data.append({
        "FinInstrmGnlAttrbts.Id": instr.find("./Id").text,
        "FinInstrmGnlAttrbts.FullNm": instr.find("./FullNm").text,
        "FinInstrmGnlAttrbts.ClssfctnTp": instr.find("./ClssfctnTp").text,
        "FinInstrmGnlAttrbts.CmmdtyDerivInd": instr.find("./CmmdtyDerivInd").text,
        "FinInstrmGnlAttrbts.NtnlCcy": instr.find("./NtnlCcy").text,
        "Issr": instr.find("./Issr").text,
    })

# Step 8: Write data to CSV file
if data:
    with open("output.csv", "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
else:
    print("No data found in XML file")

# Step 9: Upload CSV file to S3 bucket
s3 = boto3.client("s3")
# For AWS Uploading
s3.upload_file("output.csv", "my-bucket", "output.csv")
