import logging

from util import read_input_xml_find_files_location_to_be_downloaded, download_and_extract_zip_files, write_data_to_s3, \
    read_xml_and_convert_into_df_bytes


class ProcessXMl:
    def __init__(self):
        self.file_name = input('enter the file , must be located in local path ')
        logging.info(f"file name f{self.file_name}")

    def main(self):
        name_and_location_dict = read_input_xml_find_files_location_to_be_downloaded(self.file_name)
        for key, value in name_and_location_dict.items():
            logging.info(f" writing file {key}, from location {value}")
            download_and_extract_zip_files(value, key)
            write_data_to_s3(read_xml_and_convert_into_df_bytes(key[:-3] + "xml"),
                             bucket="priyaa",
                             key=key[:-4] )

            logging.info(f"wrote back to s3 f{key[:-3]}.csv")


if __name__ == '__main__':
    ProcessXMl().main()
