import threading
import time
import uuid
from container_handler import build_container, repo2docker_container
from sqs_queue_utils import get_message


class TaskManager:
    """Manager for threads and tasks.

    Parameters:
    max_threads (int): Maximum number of threads to run.
    idle_time (int): Time to wait before idling a thread.
    kill_time (int): Time to wait before killing a thread.
    max_retry (int): Max number of retries for a function
    """
    def __init__(self, max_threads=5, kill_time=180, max_retry=1):
        self.max_threads = max_threads
        self.kill_time = kill_time
        self.max_retry = max_retry
        self.thread_status = {}
        self.total_threads = 0

    def execute_work(self):
        """Pulls down a message from SQS and performs a task. Automatically
        dies when it hasn't performed a task in self.kill_time seconds.
        """
        thread_id = str(uuid.uuid4())
        self.thread_status[thread_id] = "WORKING"
        START_TIME = time.time()
        while True:
            if time.time() - START_TIME >= self.kill_time:
                break
            else:
                task = get_message()
                if task is not None:
                    function_name = task.pop("function_name")

                    args = []
                    for arg in task:
                        args.append(task[arg])

                    if function_name == "build_container":
                        attempt_num = 0
                        while attempt_num <= self.max_retry:
                            try:
                                build_container(*args)
                                break
                            except:
                                attempt_num += 1
                    else:
                        break
                    START_TIME = time.time()
                else:
                    self.thread_status[thread_id] = "IDLE"
                    continue

            time.sleep(5)

        self.total_threads -= 1
        del self.thread_status[thread_id]
        return

    def start_thread(self):
        """Starts a thread to do work.
        """
        if self.total_threads < self.max_threads:
            threading.Thread(target=self.execute_work).start()
            self.total_threads += 1
