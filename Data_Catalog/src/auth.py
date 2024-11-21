import pydata_google_auth as gauth
from logging_module import setup_logging

class GoogleCloudAuth:
    SCOPES = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]

    def __init__(self):
        self.logger = setup_logging()
        self.credentials = None

    def authenticate(self):
        """Get Google Cloud credentials."""
        self.logger.info("Attempting to authenticate with Google Cloud")
        try:
            self.credentials = gauth.get_user_credentials(
                self.SCOPES,
                auth_local_webserver=False,
            )
            self.logger.info("Successfully authenticated with Google Cloud")
            return self.credentials
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}", exc_info=True)
            raise

    def get_credentials(self):
        """Return existing credentials or authenticate to get new ones."""
        if not self.credentials:
            self.credentials = self.authenticate()
        return self.credentials