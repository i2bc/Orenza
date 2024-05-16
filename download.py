from ftplib import FTP
import sys
import bs4
import requests
import os
from bs4 import BeautifulSoup


def http(url, local_file):
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

        with open(local_file, "wb") as file:
            for chunk in response.iter_content(1024):
                if chunk:
                    file.write(chunk)
        return True

    except requests.exceptions.RequestException as e:
        print(f"Download failed: {e}")
        return False


def ftp(ftp_host: str, remote_file: str, local_file: str, ftp_user="anonymous", ftp_passwd="anonymous@"):
    """
    Download a file from the specified URL using the ftp protocol

    Args:
        ftp_host: Name of the host
        remote_file: path to the remote file excluding the ftp_host name
        local_file: name and location of the downloaded file
        ftp_user: name to use if needed to connect, anonymous by default
        fpt_passwd: password to use if needed to connect, anonymous@ by default
    """
    try:
        # Connect to the FTP server
        ftp = FTP(ftp_host)
        ftp.login(user=ftp_user, passwd=ftp_passwd)
        print(ftp_host)

        # Download the file
        with open(local_file, "wb") as local_fp:
            ftp.retrbinary("RETR " + remote_file, local_fp.write)

        # Close the FTP connection
        ftp.quit()
        # TODO Print to log
        print(f"File '{remote_file}' downloaded successfully as '{local_file}'")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


# PDB


def https(url: str, filename: str):
    """
    Download file from an url using https
    Args:
        url: link to the https to download_file
        filename: name of the downloaded file
    """
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
    else:
        basename = os.path.basename(filename)
        print(f"Failed to download_file {basename}")


def pdb_get_subfolder(url: str):
    """
    Get a list of the links (url) on the page
    Args:
        url: link to the page

    """
    r = requests.get(url)
    html_data = r.content

    parsed_data = BeautifulSoup(html_data, "html.parser")

    links = parsed_data.find_all("a", href=True)
    folders = []
    for link in links:
        if link.get("href"):
            if len(link["href"]) == 3:  # Ensure that the link is of the format letter/digit letter/digit
                if link["href"] not in folders:
                    folders.append(link["href"])
    return folders


def pdb_download_subfolder(base_url: str, output_path: str, folder: str):
    """
    download all the content contained in the specified subfolder
    Args:
        base_url: the page containing the subfolder
        output_path: path where the folders will be download
        folder: name of the folder to download
    """
    subfolder_url = os.path.join(base_url, folder)
    subfolder_name = folder[:-1]
    full_subfolder_name = os.path.join(output_path, subfolder_name)
    if not os.path.exists(full_subfolder_name):
        os.makedirs(full_subfolder_name)

    r = requests.get(subfolder_url)
    html_data = r.content
    parsed_data = BeautifulSoup(html_data, "html.parser")
    links = parsed_data.find_all("a", href=true)
    for link in links:
        if link.get("href"):
            if link["href"].endswith("xml.gz"):
                full_url = os.path.join(subfolder_url, link["href"])
                full_name = os.path.join(full_subfolder_name, link["href"])
                https(full_url, full_name)


# if __name__ == "__main__":
#    url = "https://www.enzyme-database.org/downloads/enzyme-data.xml.gz"
#    filename = "explorenz_data.xml.gz"
#    success = http(http, filename)
#
#    if success:
#        print(f"File downloaded successfully: {filename}")
#    else:
#        print("Download failed!")

# if __name__ == "__main__":
#    # FTP server credentials
#    ftp_host = "ftp.expasy.org"
#    # ftp_user = 'username'
#    # ftp_passwd = 'password'
#
#    # Remote and local file paths
#    path_file = "/databases/uniprot/current_release/knowledgebase/complete/"
#    remote_file = "uniprot_sprot.dat.gz"
#    local_file = "uniprot_sprot.dat.gz"
#
#    # Download the file with error handling
#    ftp_file(ftp_host, path_file, remote_file, local_file)
