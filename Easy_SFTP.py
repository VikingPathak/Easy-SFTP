# -*- coding: utf-8 -*-
"""
Author: @VikingPathak
"""


import os
import pysftp
import logging

logger = logging.getLogger(__name__)


class SFTP_Server:
    '''
    The `SFTP_Server` is built on top of `pysftp` library. It provides methods to connect and perform
    upload, download, move and other operations on a SFTP server.
    '''

    def __init__(self, host:str=None, user:str=None, password:str=None) -> None:
        '''
        Performs all the process related to the SFTP server related to file systems.

        Parameters:
        ---
        host : str
            SFTP host to be connected

        user : str
            SFTP user to be connected

        password : str
            SFTP Server Password

        Returns:
        ---
        None
        '''

        ### PROMPT FOR SFTP HOST IF NOT PROVIDED ##############################
        if not host:
            host = input("Enter SFTP HOST     : ")

        ### PROMPT FOR SFTP USER IF NOT PROVIDED ##############################
        if not user:
            user = input("Enter SFTP USERNAME : ")

        ### PROMPT FOR SFTP PASSWORD IF NOT PROVIDED ##########################
        if not password:
            import getpass
            password = getpass.getpass("Enter SFTP PASSWORD : ")

        self.host = host
        self.user = user

        ### ESTABLISH THE SFTP CONNECTION #####################################
        try:
            self.conn = pysftp.Connection(
                host     = self.host ,
                username = self.user ,
                password = password  ,
            )
            logger.info(f"Connection to the host {self.host} established successfully by user {self.user}")
        except:
            self.conn = None
            logger.exception(f"Connection to the host {self.host} for user {self.user} encountered an error")

    
    def close(self) -> None:
        '''
        Closes the current SFTP connection

        Returns:
        ---
        None
        '''

        try:
            self.conn.close()
            logger.info(f"Connection to host {self.host} closed successfully for user {self.user}")
        except:
            logger.exception(f"Closing connection to host {self.host} for user {self.user} encountered an error")

    
    def get_files(self, remotepath:str=None) -> list:
        '''
        Get the list of files present in the `remotepath` directory of the server.

        Parameters:
        ---
        remotepath : str
            The directory on the SFTP server to look for.

        Returns:
        ---
        list
            List consisting of all the folder names and file names
            present in the remotepath directory.
        '''

        if not remotepath: return self.conn.listdir()

        with self.conn.cd(os.path.dirname(remotepath)):
            return self.conn.listdir()

    
    def download_file(self, remote_filepath:str, local_dir:str=None) -> bool:
        '''
        Download file(s) from the mentioned server directory.

        Parameters:
        ---
        remote_filepath : str
            Name of the file with extension and path on the server to be downloaded.

        local_dir : str
            The local directory where the file will be downloaded.

        Returns:
        ---
        bool
            True, if download is successful, else False
        '''

        try:
            with self.conn.cd(os.path.dirname(remote_filepath)):
                self.conn.get(
                    remotepath = remote_filepath, 
                    localpath  = os.path.join(local_dir, os.path.basename(remote_filepath))
                )
            return True
        except:
            logger.exception(f"Downloading file {remote_filepath} from the server {self.host} encountered an error")

        return False
        

    def upload_file(self, local_filepath:str, remotepath:str):
        '''
        Description:
        ---
        Upload the files to the SFTP server

        Parameters:
        ---
        local_filepath : str
            Name of the local file along with the path to be uploaded.

        remotepath : str
            The directory on the SFTP server where you want to place this request file.

        Returns:
        ---
        bool
            True if file is uploaded, else False
        '''

        try:
            with self.conn.cd(remotepath):
                self.conn.put(localpath = local_filepath)
            return True
        except:
            logger.exception(f"Uploading file {local_filepath} to the server {self.host} encountered an error")

        return False


    def move_file(self, curr_filepath:str, new_filepath:str):
        '''
        Moves a file from one directory to another in the SFTP server.

        Parameters:
        ---
        curr_filepath : str
            The current filepath on the server along with the file extension that needs to be moved.

        new_filepath : str
            The new filepath with the extension where the file needs to be moved on the server.

        Returns:
        ---
        bool
            True, if moved successfully, else False
        '''

        try:
            self.conn.rename(curr_filepath, new_filepath)
            return True
        except:
            logger.exception(f"Moving file {curr_filepath} to {new_filepath} on server {self.host} encountered an error")

        return False
