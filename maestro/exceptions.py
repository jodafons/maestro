

class MaestroError(Exception):
    """Base class for errors raised by Qiskit."""

    def __init__(self, *message):
        """Set the error message."""
        super().__init__(" ".join(message))
        self.message = " ".join(message)

    def __str__(self):
        """Return the message."""
        return repr(self.message)



class MaestroRemoteCreationError(MaestroError):
    """Raised when an error"""

    message = "its not possible to create a session. please set the remote first."


class MaestroConnectionError(MaestroError):
    """Raised when an error"""

    message = "the server connection is not found."

class MaestroTokenNotValidError(MaestroError):
    """Raised when an error"""

    message = "the token is not valid."

class MaestroDownloadError(MaestroError):
    """Raised when an error"""

    message = "failed to download content from server"