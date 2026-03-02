import threading, time
import prometheus_client
from prometheus_client import Gauge

# Create a registry
g = Gauge('test_metric', 'desc', ['id', 'foo'])
lock = threading.Lock()
cache = {}

def model():
    with lock:
        time.sleep(0.001) # Simulate some work
    return cache

def update():
    while True:
        with lock:
            for k in list(cache.keys()):
                val = cache[k]
                g.labels(**{'id': k, 'foo': 'bar'}).set(val)
        time.sleep(1)

def simulate_scrape():
    while True:
        # Simulate prometheus scrape
        list(prometheus_client.REGISTRY.collect())
        time.sleep(0.5)

cache.update({str(i): i for i in range(5000)})

# Thread 1: continuous update holding lock
t1 = threading.Thread(target=update)
t1.daemon = True
t1.start()

# Thread 2: scrape
t2 = threading.Thread(target=simulate_scrape)
t2.daemon = True
t2.start()

start = time.time()
while time.time() - start < 10:
    time.sleep(1)
print("Finished without deadlock!")
