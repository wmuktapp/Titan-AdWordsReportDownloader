"""Download Adwords reports based on a JSON input and upload directly to either FTP or Titan's blob storage.

Request a report per client customer ID via the Adwords API and directly upload to a single file.

For more detail, execute the following at the command prompt:
adwordsreportdownloader --help

"""


import datetime
import ftplib
import json
import logging
import os
import re
import sys

from googleads import adwords
import click
import yaml


class _DateType(click.ParamType):
    """Initialise a custom click type to be used to validate a date provided at command line input."""

    name = "Date (YYYY-MM-DD)"

    def convert(self, value, param, ctx):
        """Check that the input is a valid FTP connection string on a standard regex pattern.

        This method is called implicitly by click and shouldn't be invoked directly.

        Positional Arguments:
        1. value (string): the value that is to be validated / converted
        2. param (unknown): (unknown as not documented by click). This value should be passed to the second parameter of
        the fail() method
        3. ctx (unknown): (unknown as not documented by click). This value should be passed to the third parameter of
        the fail() method

        """
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            self.fail("Incorrect date format, should be YYYY-MM-DD")


class _FTPURIType(click.ParamType):
    """Initialise a custom click type to be used to validate FTP connection details provided at command line input."""

    name = "FTP Path"
    # ftp|ftps://<user>:<password>@<host>[:port][/path]
    _regex = re.compile(r"(?P<protocol>ftp|ftps)://(?P<user>.+):(?P<password>.+)@(?P<host>[^:/]+)(?::(?P<port>\d+))?"
                        r"(?:/(?P<path>.*))?")

    def convert(self, value, param, ctx):
        """Check that the input is a valid FTP connection string on a standard regex pattern.

        This method is called implicitly by click and shouldn't be invoked directly.

        Positional Arguments:
        1. value (string): the value that is to be validated / converted
        2. param (unknown): (unknown as not documented by click). This value should be passed to the second parameter of
        the fail() method
        3. ctx (unknown): (unknown as not documented by click). This value should be passed to the third parameter of
        the fail() method

        """
        match = self._regex.match(value)
        if match is None:
            self.fail("%s is not a valid FTP URI" % value, param, ctx)
        else:
            return match.groups()
        
        
class _ReportStreams(object):
    def __init__(self, streams):
        """Initialise a file-like object that will handle the lazy reading of the reports in response to calls to the 
        read method.

        Positional Arguments:
        1. streams (iterable): the list of streams to be read

        """
        self.streams = []
        for stream in streams:
            self.streams.append({
                "fp": stream,
                "bytes_read": 0,   # This is the bytes read from the file
                "header_found": False
            })
        self.current_stream_index = 0
        self.end_of_streams_reached = False
        self.total_bytes_returned = 0  # This is the total bytes returned

    def read(self, block_size):
        """Read and return a block_size chunk from the next report file, handling the removal of headers from subsequent
        files.

        The final read from data may contain less than block_size bytes and subsequent calls will return an empty byte
        string.

        Positional Arguments:
        1. block_size (int): the number of bytes to read

        """
        if self.end_of_streams_reached:
            return b""
        total_bytes_read = 0
        data_to_return = b""
        while total_bytes_read < block_size and not self.end_of_streams_reached:
            stream = self.streams[self.current_stream_index]
            bytes_read = stream["bytes_read"]
            fp = stream["fp"]
            data = fp.read(block_size - total_bytes_read)
            bytes_read += len(data)
            stream["bytes_read"] = bytes_read
            data_to_return += self.process_header(data)
            total_bytes_read = len(data_to_return)
            if len(data) < block_size:
                fp.close()
                if (self.current_stream_index + 1) == len(self.streams):
                    self.end_of_streams_reached = True
                else:
                    self.current_stream_index += 1
        self.total_bytes_returned += total_bytes_read
        return data_to_return

    def process_header(self, data):
        """If the chunk that is being processed is not the first file, and the header has not yet been found, remove it
        and return the remainder.

        Positional Arguments:
        1. data (bytes): the chunk that is being processed

        """
        stream = self.streams[self.current_stream_index]
        if not stream["header_found"] and self.current_stream_index > 0:
            header_end_index = data.find(b"\n")
            if header_end_index != -1:
                stream["header_found"] = True
                header_byte_count = header_end_index + 1
            else:
                header_byte_count = len(data)
            data = data[header_byte_count:]
        return data

    def tell(self):
        """Return the total bytes read."""
        return self.total_bytes_returned


class FlowManager(object):
    THIS_DIRECTORY = os.path.dirname(__file__)

    def __init__(self, developer_token, client_id, client_secret, refresh_token, report_config, account_ids,
                 ftp_destination, ftp_file_name):
        """Initialise an object that controls the flow of the application - from chaining the API calls to upload the
        downloaded reports to FTP as a single file.

        Positional Arguments:
        1. developer_token (string): The AdWords API developer token needed for the calls
        2. client_id (string): The client ID from the Google Developer Console project credential that will be used to
        generate the authentication needed for the API calls
        3. client_secret (string): The client secret from the Google Developer Console project credential that will be used
        to generate the authentication needed for the API calls
        4. refresh_token (string): The OAuth2 refresh token used to generate access tokens. This can be generated by
        running the generate_refresh_token.py script
        5. report_config (dict): The serialised JSON-structured AdWords report configuration. For more details, see
        https://developers.google.com/adwords/api/docs/guides/reporting#create_a_report_definition
        6. account_ids (list): A list of AdWords Account IDs (aka client customer IDs) to request reports for
        7. ftp_destination (tuple): FTP connection details in the format of (protocol, user, pass, host, port, path) -
        port and path can be None
        If this is not provided, it is assumed that the execution environment is Titan in which case, the file will be
        uploaded to blob storage
        8. ftp_file_name (string): The name of the file to write to FTP. If --ftp-destination is not provided, this is
        ignored
        9. disable_ssl_verification (bool): Whether or not SSL verification should be disabled (defaults to False)

        """
        self.developer_token = developer_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.client = None
        self.report_config = report_config
        self.account_ids = account_ids
        self.ftp_destination = ftp_destination
        self.ftp_file_name = ftp_file_name
        self.logger = logging.getLogger(__name__)
        self._configure_logger()

    def _configure_logger(self):
        """Configure the instance-level logger object's format and handlers."""
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S")
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        file_handler = logging.FileHandler(os.path.join(self.THIS_DIRECTORY, "output.log"))
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def authenticate(self):
        """Add authentication details to a YAML string and then initialise the adwords.AdWordsClient object.

        Creates a YAML-formatted string, starting with the contents of the googleads.yaml file if it exists, and adds
        the developer_token, client_id, client_secret and refresh_token details.

        """
        yaml_file_path = os.path.join(self.THIS_DIRECTORY, "googleads.yaml")
        if os.path.isfile(yaml_file_path):
            with open(yaml_file_path) as stream:
                yaml_file = yaml.load(stream)
        else:
            yaml_file = {}
        adwords_key = 'adwords'
        adwords_config = yaml_file.get(adwords_key)
        if adwords_config is None:
            adwords_config = yaml_file[adwords_key] = {}
        adwords_config["developer_token"] = self.developer_token
        adwords_config["client_id"] = self.client_id
        adwords_config["client_secret"] = self.client_secret
        adwords_config["refresh_token"] = self.refresh_token
        self.client = adwords.AdWordsClient.LoadFromString(yaml.dump(yaml_file))

    def get_report_streams(self):
        """Connect to the AdWords API and return a dict mapping account IDs to report streams."""
        streams = {}
        # The default library is "zeep" and it makes a request. The googleads API doesn't send our disable SSL option
        # to this library and so this step fails with SSL error
        self.client.soap_impl = "suds"
        downloader = self.client.GetReportDownloader(version="v201802")
        for account_id in self.account_ids:
            self.client.SetClientCustomerId(account_id)
            streams[account_id] = downloader.DownloadReportAsStream(self.report_config, skip_report_header=True,
                                                                    skip_report_summary=True,
                                                                    include_zero_impressions=False)
        return streams

    def run(self):
        """Run the end to end report generation to upload process."""
        self.logger.info("EXECUTION STARTED")
        self.logger.info("Authenticating...")
        self.authenticate()
        self.logger.info("Getting report streams...")
        streams = self.get_report_streams()
        self.logger.info("Uploading streams...")
        self.upload_streams(streams)
        self.logger.info("File(s) succesfully uploaded")
        self.logger.info("EXECUTION ENDED")

    def upload_streams(self, streams):
        """Upload streams to a single file on FTP.

        Positional Arguments:
        1. streams (list): the list of streams to upload

        """
        protocol, user, password, host, port, path = self.ftp_destination
        path = os.path.normpath(path)
        ftp_class = ftplib.FTP if protocol == "ftp" else ftplib.FTP_TLS
        self.logger.info("Connecting to the FTP server...")
        with ftp_class() as ftp:
            ftp.connect(host, port or 0)
            ftp.login(user, password)
            if protocol == "ftps":
                ftp.prot_p()
            ftp.cwd(path)
            try:
                ftp.storbinary("STOR %s" % self.ftp_file_name, _ReportStreams(streams.values()), blocksize=1250000)
            except ftplib.all_errors as error:
                try:
                    ftp.delete(os.path.join(path, self.ftp_file_name))
                except ftplib.all_errors as delete_error:
                    self.logger.error(delete_error)
                self.logger.critical(error)


class TitanFlowManager(FlowManager):
    def __init__(self, developer_token, client_id, client_secret, refresh_token, report_config, account_ids):
        """Initialise an object that controls the flow of the application - from chaining the API calls to upload the
        downloaded reports to Titan's blob storage as a single file.

        Positional Arguments:
        1. developer_token (string): The AdWords API developer token needed for the calls
        2. client_id (string): The client ID from the Google Developer Console project credential that will be used to
        generate the authentication needed for the API calls
        3. client_secret (string): The client secret from the Google Developer Console project credential that will be used
        to generate the authentication needed for the API calls
        4. refresh_token (string): The OAuth2 refresh token used to generate access tokens. This can be generated by
        running the generate_refresh_token.py script
        5. report_config (dict): The serialised JSON-structured AdWords report configuration. For more details, see
        https://developers.google.com/adwords/api/docs/guides/reporting#create_a_report_definition
        6. account_ids (list): A list of AdWords Account IDs (aka client customer IDs) to request reports for
        7. disable_ssl_verification (bool): Whether or not SSL verification should be disabled (defaults to False)

        """
        self.developer_token = developer_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.client = None
        self.report_config = report_config
        self.account_ids = account_ids

        from titan import utilities
        self.acquire_program = utilities.AcquireProgram()
        self.logger = self.acquire_program.logger

    def upload_streams(self, streams):
        """Upload streams to Titan's blob storage - 1 blob per stream

        Positional Arguments:
        1. streams (list): the list of streams to upload

        """
        name_format = "{ExecutionDataSetName}_{ExecutionLoadDate}_{account_id}.csv"
        for account_id, stream in streams.items():
            self.logger.info("Uploading file for account ID, %s to blob storage" % account_id)
            blob_name = self.acquire_program.get_blob_name(name_format=name_format, account_id=account_id)
            self.acquire_program.create_blob_from_stream(stream, blob_name=blob_name)


@click.command()
@click.option("-t", "--developer-token", required=True, help="The AdWords API developer token needed for the calls.")
@click.option("-c", "--client-id", required=True, help="The client ID from the Google Developer Console project "
              "credential that will be used to generate the authentication needed for the API calls.")
@click.option("-s", "--client-secret", required=True, help="The client secret from the Google Developer Console "
              "project credential that will be used to generate the authentication needed for the API calls.")
@click.option("-r", "--refresh-token", required=True, help="The OAuth2 refresh token used to generate access tokens. "
              "This can be generated by running the generate_refresh_token.py script. Instructions here; "
              "https://github.com/googleads/googleads-python-lib/wiki/API-access-using-own-credentials-(installed-"
              "application-flow)")
@click.option("-i", "--report-string", required=True, help="The JSON-structured AdWords report configuration. For more "
              "details, see https://developers.google.com/adwords/api/docs/guides/reporting#create_a_report_definition")
@click.option("-a", "--account-ids", required=True, help="A space delimited string of AdWords Account IDs (aka client "
              "customer IDs) to request reports for")
@click.option("-f", "--ftp-destination", type=_FTPURIType(), help="FTP connection details in the format, "
              "ftp|ftps://<user>:<pass>@<host>[:port][/path]. If this is not provided, it is assumed that the "
              "execution environment is Titan in which case, the file will be uploaded to blob storage.")
@click.option("-o", "--ftp-file-name", help="The name of the file to write to FTP. If --ftp-destination is not "
              "provided, this is ignored")
@click.option("-l", "--load-date", type=_DateType(), help="The end date in the format YYYY-MM-DD that the report date "
              "range should reference. This is only added to the JSON if there is no dateRangeType present. Defaults "
              "to yesterday")
@click.option("-d", "--previous-days", type=int, default=90, help="The amount of days to fetch data for. The start "
              "date is equal to (--load-date - previous_days) - 1. The start date is only added to the JSON if there "
              "is no dateRangeType. Defaults to 90.")
def main(developer_token, client_id, client_secret, refresh_token, report_string, account_ids, ftp_destination,
         ftp_file_name, load_date, previous_days):
    """Download an AdWords report based on a JSON input and upload directly to either FTP or Titan's blob storage.

    Positional Arguments:
    1. developer_token (string): The AdWords API developer token needed for the calls
    2. client_id (string): The client ID from the Google Developer Console project credential that will be used to
    generate the authentication needed for the API calls
    3. client_secret (string): The client secret from the Google Developer Console project credential that will be used
    to generate the authentication needed for the API calls
    4. refresh_token (string): The OAuth2 refresh token used to generate access tokens. This can be generated by
    running the generate_refresh_token.py script
    5. report_string (string): The JSON-structured AdWords report configuration. For more details, see
    https://developers.google.com/adwords/api/docs/guides/reporting#create_a_report_definition
    6. account_ids (string): A space delimited string of AdWords Account IDs (aka client customer IDs) to request
    reports for
    7. ftp_destination (string): FTP connection details in the format, ftp|ftps://<user>:<pass>@<host>[:port][/path].
    If this is not provided, it is assumed that the execution environment is Titan in which case, the file will be
    uploaded to blob storage
    8. ftp_file_name (tuple): The name of the file to write to FTP. If --ftp-destination is not provided, this is
    ignored
    9. load_date (datetime.date): The end date that the report date range should reference. This is only added to the
    JSON if there is no dateRangeType present. Defaults to yesterday
    10. previous_days (int): The amount of days to fetch data for. The start date is equal to
    (--load-date - previous_days) - 1. The start date is only added to the JSON if there is no dateRangeType. Defaults
    to 90.

    """
    flow_manager = None
    try:
        account_ids = account_ids.split()
        report_config = json.loads(report_string)
        if ftp_destination is not None:
            # Pass an empty dict as report_config as it will be set later
            flow_manager = FlowManager(developer_token, client_id, client_secret, refresh_token, {}, account_ids,
                                       ftp_destination, ftp_file_name)
            if ftp_file_name is None:
                flow_manager.logger.critical("FTP file name must be provided if ftp_destination is provided")
        else:
            flow_manager = TitanFlowManager(developer_token, client_id, client_secret, refresh_token, {}, account_ids)
        try:
            if "dateRangeType" not in report_config:
                report_config["dateRangeType"] = "CUSTOM_DATE"
                if load_date is None:
                    load_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
                start_date = (load_date - datetime.timedelta(days=previous_days)).strftime("%Y-%m-%d")
                report_config["selector"]["dateRange"] = {"min": start_date, "max": str(load_date)}
            flow_manager.report_config = report_config
        except KeyError as error:
            flow_manager.logger.exception(error)
        flow_manager.run()
    except Exception as error:
        try:
            flow_manager.logger.exception(error)
        finally:
            sys.exit("ERROR ENCOUNTERED - CHECK LOGS")
