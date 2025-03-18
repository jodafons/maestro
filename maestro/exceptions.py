

class MaestroRuntimeError(Exception):
    """Base class for errors raised by Qiskit."""

    def __init__(self, *message):
        """Set the error message."""
        super().__init__(" ".join(message))
        self.message = " ".join(message)

    def __str__(self):
        """Return the message."""
        return repr(self.message)

class MaestroRemoteCreationError(MaestroRuntimeError):
    """Raised when an error"""

    message = "its not possible to create a session. please set the remote first."

class MaestroConnectionError(MaestroRuntimeError):
    """Raised when an error"""

    message = "the server connection is not found."

class MaestroTokenNotValidError(MaestroRuntimeError):
    """Raised when an error"""

    message = "the token is not valid."

class MaestroDownloadError(MaestroRuntimeError):
    """Raised when an error"""

    message = "failed to download content from server"
    
    