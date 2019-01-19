import threading
from heapq import heappush, heappop
import traceback
import sys


def get_tid():
    return id(threading.current_thread())


class SharedState:
    def __init__(self):
        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)
        self.current_time = 0.0
        self.current_tid = None
        self.event_queue = []
        self.unhandled_exception = None
        self.tid_to_thread = {}
        self.current_thread = None


class Queue:
    def __init__(self, context):
        self.elements = []
        self.element_added = Event(context)
        self.context = context

    def take(self):
        'gets next element, or blocks if there is none'
        while len(self.elements) == 0:
            self.context.wait(None, events=[self.element_added])

        e = self.elements[0]
        del self.elements[0]

        return e

    def put(self, element):
        self.elements.append(element)
        self.element_added.notify()


class Event:
    def __init__(self, context, name=None):
        self.wait_list = set()
        self.context = context
        self.notify_count = 0
        self.name = name

    # the update to notify_count is not thread safe, but... currently not important.
    def notify(self):
        self.context.next_event(self)
        self.notify_count += 1

    def __repr__(self):
        return "Event(waiting={}, notify_count={})".format(self.wait_list, self.notify_count)


class Thread:
    def __init__(self, context, run, name=None):
        self.context = context
        self.run = run
        self.name = name
        self.thread = threading.Thread(target=self._thread_adapter, name=name)
        self.thread.daemon = True
        self.registration_complete = threading.Event()

    def _thread_adapter(self):
        self.context.register_new_thread(self)
        self.context.log("starting {}".format(self.run))
        try:
            self.run()
        except Exception:
            ex_type, ex, tb = sys.exc_info()
            self.context.shared.unhandled_exception = (
                ex, traceback.extract_tb(tb))
        finally:
            self.context.log("finished {}".format(self.run))
            self.context.unregister_thread()
        #print("_thread_adapter ended")

    def start(self):
        self.thread.start()
        self.registration_complete.wait()


class Context:
    def __init__(self, shared):
        self.shared = shared
        self.counter = 0

    def log(self, msg):
        tname = None
        if self.shared.current_thread is not None:
            tname = self.shared.current_thread.name
        print("  thread {}: {}".format(tname, msg))

    def create_thread(self, run_func, name=None):
        return Thread(self, run_func, name=name)

    def now(self):
        return self.shared.current_time

    # this function is thread-safe
    def future_event(self, delay, event=None):
        self.shared.lock.acquire()
        timestamp = self.now() + delay
        if event is None:
            event = Event(self)

        heappush(self.shared.event_queue, (timestamp, self.counter, event))
        self.counter += 1

        # self.log("pushed event @{}: {} (queue len={})".format(
        #    timestamp, event, len(self.shared.event_queue)))
        self.shared.lock.release()

        return event

    # this function is thread-safe
    def next_event(self, event):
        self.future_event(0, event)

    def register_new_thread(self, threadobj):
        tid = id(threading.current_thread())

        # construct event manually to queue up first execution of this thread
        start_event = Event(self)
        start_event.wait_list.add(tid)
        self.future_event(0, start_event)

        # now wait for the main loop to say it's our turn
        self.shared.lock.acquire()
        self.shared.tid_to_thread[tid] = threadobj

        # record that we're done with registration and ready to wait for execution
        threadobj.registration_complete.set()

        # sleep until this thread gets a chance to start running
        while self.shared.current_tid != tid:
            self.shared.changed.wait()
        self.shared.lock.release()

    def unregister_thread(self):
        tid = id(threading.current_thread())
        shared = self.shared

        shared.lock.acquire()
        shared.current_tid = None  # indicate no thread is running
        shared.changed.notify_all()
        del self.shared.tid_to_thread[tid]
        shared.lock.release()

    def wait(self, timeout=None, events=[]):
        tid = get_tid()
        try:
            assert self.shared.current_tid == tid
        except:
            print("tid mismatch!!!")
            raise

        if timeout is not None:
            events = events + [self.future_event(timeout)]

        for e in events:
            e.wait_list.add(tid)

        self.shared.lock.acquire()
        #self.log("wait setting current_id = None")
        self.shared.current_tid = None  # indicate no thread is running
        self.shared.changed.notify_all()

        while self.shared.current_tid != tid:
            self.shared.changed.wait()
        self.shared.lock.release()

        for e in events:
            e.wait_list.remove(tid)


class UncaughtException(Exception):
    pass


def event_loop(shared):
    event_queue = shared.event_queue
    while len(event_queue) > 0:
        timestamp, _, next_event = heappop(event_queue)

        shared.current_time = timestamp

#        print("timestamp={}: tids waiting={}".format(
#            timestamp, next_event.wait_list))
        for tid in list(next_event.wait_list):
            threadobj = shared.tid_to_thread[tid]
#            print("timestamp={}: starting tid={}, name={}".format(
#                timestamp, tid, threadobj.name))
            assert tid is not None

            shared.lock.acquire()
            shared.current_tid = tid  # update with the thread which is allowed to run now
            shared.current_thread = threadobj
            shared.changed.notify_all()

            # unlock will signal tid can start running
            # wait until tid becomes None again
            while shared.current_tid is not None:
                #print("main loop: waiting current_id={}".format(shared.current_tid))
                shared.changed.wait()
            unhandled_exception = shared.unhandled_exception
            shared.current_thread = None
            shared.lock.release()
            #print("timestamp={} pausing tid={}".format(timestamp, tid))

            if unhandled_exception is not None:
                print("".join(traceback.format_list(unhandled_exception[1])))
                print(unhandled_exception[0])
                raise UncaughtException(unhandled_exception[0])
        #print("events remaining", len(shared.event_queue))
