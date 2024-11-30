import multiprocessing
import time
import os

def background_task():
    log_path = "C:\\Users\\ivanh\\Desktop\\background_task.log"
    print(f"Current working directory: {os.getcwd()}")
    print(f"Writing log to: {log_path}")

    while True:
        try:
            with open(log_path, "a") as log:
                log.write("hello\n")
            print("Log written successfully.")
        except Exception as e:
            print(f"Error writing to log file: {e}")
        time.sleep(10)

if __name__ == "__main__":
    # Start the background task in a separate process
    process = multiprocessing.Process(target=background_task)
    process.daemon = True  # This makes it a daemon process (it will not block the script's exit)
    process.start()

    print(f"Background task started with PID {process.pid}")
    print("You can continue using your terminal.")

    # Keep the main script running to ensure the background task continues
    while True:
        time.sleep(1)  # Keep the main script alive
