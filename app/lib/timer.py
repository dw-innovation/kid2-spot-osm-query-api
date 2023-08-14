import time


class Timer:
    def __init__(self):
        # Dictionary to store the checkpoints with their respective times.
        self.checkpoints = {}
        self.start_time = time.time()

    def add_checkpoint(self, checkpoint_name):
        """Add a checkpoint with the current time."""
        elapsed_time = time.time() - self.start_time
        self.checkpoints[checkpoint_name] = elapsed_time
        return elapsed_time

    def get_checkpoint(self, checkpoint_name):
        """Return the elapsed time of a specific checkpoint."""
        return self.checkpoints.get(checkpoint_name, None)

    def reset(self):
        """Reset the timer and all the checkpoints."""
        self.start_time = time.time()
        self.checkpoints = {}

    def get_all_checkpoints(self):
        """Return all checkpoints."""
        return self.checkpoints
