import random
import redis
import threading
import multiprocessing as mp
import subprocess
import time

HOST = "localhost"
PROB_GET = 0.8
SIZE_GET_KEYSPACE = 3750000  # 3.75 million
SIZE_SET_KEYSPACE = 3000000  # 3 million
BYTES = memoryview(b"0" * 3000)

default_client = redis.Redis(HOST)


def generate_key_set():
    return random.randint(1, SIZE_SET_KEYSPACE + 1)


def generate_key_get():
    return random.randint(1, SIZE_GET_KEYSPACE + 1)


def generate_payload_size():
    val = random.normalvariate(1024, 400)
    positive_val = max(int(val), 2)
    valid_val = min(positive_val, len(BYTES) - 1)
    return valid_val


def should_get():
    return random.random() < PROB_GET


def warmup():
    print("starting warmup")
    default_client.flushall()
    pipe = default_client.pipeline()
    for key in range(1, SIZE_SET_KEYSPACE + 1):
        pipe.set(key, BYTES[0 : generate_payload_size()])
        if key % 1000 == 0:
            pipe.execute()
    print("completed warmup")


def pipeline_test(pipeline_size, total_commands, with_hiredis=True, transaction=True):
    assert (
        total_commands % pipeline_size == 0
    ), "Error: total_commands is not divisible by pipeline_size"
    warmup()
    print(f"starting pipeline_test(pipeline_size = {pipeline_size},", end=" ")
    print(
        f"total_commands = {total_commands}, with_hiredis = {with_hiredis}, transaction = {transaction})"
    )
    if not with_hiredis:
        subprocess.run("pip uninstall hiredis", shell=True)
    num_pipelines = total_commands // pipeline_size
    start = time.monotonic()
    for _ in range(num_pipelines):
        # client.pipeline(transaction = True) is equivalent to client.pipeline()
        pipe = default_client.pipeline(transaction=transaction)
        for i in range(pipeline_size):
            if should_get():
                pipe.get(generate_key_get())
            else:
                pipe.set(generate_key_set(), BYTES[0 : generate_payload_size()])
        pipe.execute()
    duration = time.monotonic() - start
    if not with_hiredis:
        subprocess.run("pip install hiredis", shell=True)
    print(f"Pipeline Size: {pipeline_size}, TPS: {total_commands/duration:.2f}")


def worker_thread(num_commands):
    for _ in range(num_commands):
        if should_get():
            default_client.get(generate_key_get())
        else:
            default_client.set(generate_key_set(), BYTES[0 : generate_payload_size()])


def multithreaded_test(num_threads, total_commands):
    assert (
        total_commands % num_threads == 0
    ), "Error: total_commands is not divisible by num_threads"
    warmup()
    print(
        f"starting multithreaded_test(num_threads = {num_threads}, total_commands = {total_commands})"
    )
    threads = list()
    num_commands_per_thread = total_commands // num_threads
    start = time.monotonic()
    for _ in range(num_threads):
        thread = threading.Thread(target=worker_thread, args=(num_commands_per_thread,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    duration = time.monotonic() - start
    print(f"Threads: {num_threads}, TPS: {total_commands/duration:.2f}")


def worker_process(num_commands):
    client = redis.Redis(HOST)
    for _ in range(num_commands):
        if should_get():
            client.get(generate_key_get())
        else:
            client.set(generate_key_set(), BYTES[0 : generate_payload_size()])


def multiprocess_test(num_processes, total_commands):
    assert (
        total_commands % num_processes == 0
    ), "Error: total_commands is not divisible by num_processes"
    warmup()
    print(
        f"starting multiprocess_test(num_processes = {num_processes}, total_commands = {total_commands})"
    )
    processes = list()
    num_commands_per_processes = total_commands // num_processes
    start = time.monotonic()
    for _ in range(num_processes):
        process = mp.Process(target=worker_process, args=(num_commands_per_processes,))
        processes.append(process)
        process.start()
    for process in processes:
        process.join()
    duration = time.monotonic() - start
    print(f"Processes {num_processes}, TPS: {total_commands/duration:.2f}")


def newconnection_percommand_test(total_commands):
    warmup()
    print(
        f"starting new connection per command test(total_commands = {total_commands})"
    )
    start = time.monotonic()
    for _ in range(total_commands):
        client = redis.Redis(HOST)
        if should_get():
            client.get(generate_key_get())
        else:
            client.set(generate_key_set(), BYTES[0 : generate_payload_size()])
    duration = time.monotonic() - start
    print(f"New connection for every command TPS: {total_commands/duration:.2f}")


# Pipeline tests with hiredis installed
pipeline_test(3, 6000000)
pipeline_test(10, 10000000)
pipeline_test(100, 20000000)
pipeline_test(1000, 20000000)

# Pipeline tests without hiredis installed
pipeline_test(3, 6000000, with_hiredis=False)
pipeline_test(10, 10000000, with_hiredis=False)
pipeline_test(100, 20000000, with_hiredis=False)
pipeline_test(1000, 20000000, with_hiredis=False)

# Pipeline tests with transcation set to False
pipeline_test(3, 6000000, transaction=False)
pipeline_test(10, 10000000, transaction=False)
pipeline_test(100, 20000000, transaction=False)
pipeline_test(1000, 20000000, transaction=False)

# Multithreaded tests
multithreaded_test(2, 5000000)
multithreaded_test(3, 9000000)
multithreaded_test(10, 9000000)

# Multiprocess tests
multiprocess_test(2, 9000000)
multiprocess_test(3, 9000000)
multiprocess_test(10, 15000000)
multiprocess_test(20, 15000000)
multiprocess_test(30, 15000000)
multiprocess_test(100, 15000000)

# New connection for every command tests
newconnection_percommand_test(5000)
