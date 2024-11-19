import time


class Timer:
    def __init__(self):
        # Dictionary to store the checkpoints with their respective elapsed times in ms.
        self.checkpoints = {}
        self.last_checkpoint_time = time.time()

    def add_checkpoint(self, checkpoint_name):
        """Add a checkpoint with the time elapsed since the last checkpoint."""
        current_time = time.time()

        # Convert to milliseconds and round to the nearest integer
        elapsed_time = round((current_time - self.last_checkpoint_time) * 1000)
        self.last_checkpoint_time = current_time  # Update the last checkpoint time
        self.checkpoints[checkpoint_name] = elapsed_time
        return elapsed_time

    def get_checkpoint(self, checkpoint_name):
        """Return the elapsed time of a specific checkpoint."""
        return self.checkpoints.get(checkpoint_name, None)

    def reset(self):
        """Reset the timer and all the checkpoints."""
        self.last_checkpoint_time = time.time()
        self.checkpoints = {}

    def get_all_checkpoints(self):
        """Return all checkpoints."""
        return self.checkpoints
