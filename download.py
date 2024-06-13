from ftplib import FTP
import sys
import requests
import os
from bs4 import BeautifulSoup
import logging
from time import sleep


# TODO: delete or https
def http(url, filename, logger):
    """
    Downloads a file from the specified URL and saves it with the given filename (using http link).

    Args:
        url: The URL of the file to download.
        local_file: The name of the file to save the downloaded content as.

    Returns:
        True if the download was successful, False otherwise.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for non-200 status codes

        with open(filename, "wb") as file:
            for chunk in response.iter_content(1024):
                if chunk:
                    file.write(chunk)
        return True

    except requests.exceptions.RequestException as e:
        logger.exception(e)
        return False


def ftp(
    ftp_host: str,
    remote_file: str,
    local_file: str,
    logger,
    ftp_user="anonymous",
    ftp_passwd="anonymous@",
):
    """
    Download a file from the specified URL using the ftp protocol

    Args:
        ftp_host: Name of the host
        remote_file: path to the remote file excluding the ftp_host name
        local_file: name and location of the downloaded file
        ftp_user: name to use if needed to connect, anonymous by default
        ftp_passwd: password to use if needed to connect, anonymous@ by default
    """
    try:
        # Connect to the FTP server
        ftp = FTP(ftp_host)
        ftp.login(user=ftp_user, passwd=ftp_passwd)

        # Download the file
        with open(local_file, "wb") as local_fp:
            ftp.retrbinary("RETR " + remote_file, local_fp.write)

        # Close the FTP connection
        ftp.quit()
    except Exception as e:
        logger.exception("Ftp exception: ", e)
        sys.exit(1)


# PDB
def retry_request(url, retries=3, backoff_factor=0.3):
    """
    Make a GET request to a URL with retries.
    Args:
        url: URL to make request to.
        retries: Number of retries.
        backoff_factor: Time factor for exponential backoff.
    Returns:
        Response object if successful, None otherwise.
    """
    for i in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logging.error(f"Attempt {i+1} failed for URL {url}: {e}")
            sleep(backoff_factor * (2**i))
    return None


def pdb_get_subfolder(url: str):
    """
    Get a list of the links (url) on the page.
    Args:
        url: Link to the page.
    Returns:
        List of subfolder names.
    """
    response = retry_request(url)
    if response:
        html_data = response.content
        parsed_data = BeautifulSoup(html_data, "html.parser")
        links = parsed_data.find_all("a", href=True)
        folders = [link["href"] for link in links if len(link["href"]) == 3]
        return list(set(folders))
    return []


def pdb_download_subfolder(base_url: str, output_path: str, folder: str):
    """
    Download all the content contained in the specified subfolder.
    Args:
        base_url: The page containing the subfolder.
        output_path: Path where the folders will be downloaded.
        folder: Name of the folder to download.
    """
    subfolder_url = os.path.join(base_url, folder)
    subfolder_name = folder.rstrip("/")
    full_subfolder_path = os.path.join(output_path, subfolder_name)

    if not os.path.exists(full_subfolder_path):
        os.makedirs(full_subfolder_path)

    response = retry_request(subfolder_url)
    if response:
        html_data = response.content
        parsed_data = BeautifulSoup(html_data, "html.parser")
        links = parsed_data.find_all("a", href=True)

        for link in links:
            if link["href"].endswith("xml.gz"):
                full_url = os.path.join(subfolder_url, link["href"])
                full_name = os.path.join(full_subfolder_path, link["href"])
                download_file(full_url, full_name)


def download_file(url, local_path):
    response = retry_request(url)
    if response:
        with open(local_path, "wb") as f:
            f.write(response.content)
        logging.info(f"Downloaded {url} to {local_path}")
