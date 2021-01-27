import logging
import zipfile
from io import StringIO
import boto3

import requests
from bs4 import BeautifulSoup, Tag
import os
import pandas as pd

"""This is going to write data to s3, inputs would be buffer object, bucket name,and the filename(without extension), 
also include aws_access key and secret in the environmental variable or pass it as a parameter"""


def write_data_to_s3(csv_buffer: bytes, bucket: str, key: str) -> dict:
    try:
        s3_resource = boto3.resource("s3",
                                     region_name='ap-south-1',
                                     )

        return s3_resource.Object(bucket, f'{key}.csv').put(Body=csv_buffer.getvalue())
    except Exception as e:
        logging.error(e)
        return {"error": e}


"""This is going to take the file name as an input and parse the xml into a dataframe, the instuctions were giving on 
which fields to be picked, I am returning a buffered object  """


def read_xml_and_convert_into_df_bytes(filename) -> bytes:
    try:
        soup = BeautifulSoup(open(filename, 'r').read(), 'lxml')
        data = []

        for i in soup.find('fininstrmrptgrefdatadltarpt'):
            temp = {}
            if isinstance(i, Tag):
                if i.fininstrmgnlattrbts:
                    temp['FinInstrmGnlAttrbts.Id'] = i.id.text
                    temp['FinInstrmGnlAttrbts.FullNm'] = i.fullnm.text
                    temp['FinInstrmGnlAttrbts.ClssfctnTp'] = i.clssfctntp.text
                    temp['FinInstrmGnlAttrbts.CmmdtyDerivInd'] = i.cmmdtyderivind.text
                    temp['FinInstrmGnlAttrbts.NtnlCcy'] = i.ntnlccy.text

                if i.issr:
                    temp['issr'] = i.issr.text
                else:
                    temp['issr'] = ""
                data.append(temp)

        csv_buffer = StringIO()
        pd.DataFrame(data).to_csv(csv_buffer)
        logging.info(data)
        return csv_buffer
    except FileNotFoundError:
        logging.error(f"file not found {filename}")
        return {"error": f"file not found,checking at {os.getcwd()}"}


"""give me the url,and a filename for storing the zip and later extracting the same """


def download_and_extract_zip_files(url: str, filename: str):
    try:
        r = requests.get(url, allow_redirects=True)
        open(filename, 'wb').write(r.content)
        logging.info("successs")
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall('')
        logging.info(f"extracted zips f{filename}")
    except Exception as e:
        logging.error(e)
        print(f"error {e}")


"""AN input xml has been provided which contains list of location from which we need to download zzips"""


def read_input_xml_find_files_location_to_be_downloaded(filename) -> dict:
    try:
        soup = BeautifulSoup(open(filename, 'r').read(), 'lxml')
        download_link = [i.text for i in soup.find_all("str", {"name": "download_link"})]
        file_name = [i.text for i in soup.find_all("str", {"name": "file_name"})]
        logging.info(filename)
        return dict(zip(file_name, download_link))

    except FileNotFoundError:
        logging.error(f"file not found,checking at {os.getcwd()}")
        return {"error": f"file not found,checking at {os.getcwd()}"}
    except Exception as e:
        return {"error": str(e)}
