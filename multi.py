from multiprocessing import Process, Lock
from time import sleep

def f(l, i):
    l.acquire()
    try:
        sleep(0.3)
        print('hello world', i)
    finally:
        l.release()

if __name__ == '__main__':
    lock = Lock()

    for num in range(10):
        Process(target=f, args=(lock, num)).start()