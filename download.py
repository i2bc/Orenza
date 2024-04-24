from ftplib import FTP
import sys
import requests


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


def ftp(ftp_host, remote_file, local_file, ftp_user="anonymous", ftp_passwd="anonymous@"):
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
