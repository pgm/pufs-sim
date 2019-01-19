from simplat import Thread, SharedState, Context, Queue, Event, event_loop
from sim import ObjectReadReq, execute_obj_read


class Globals:
    def __init__(self, context):
        self.read_length = 1024  # number of bytes object read request returns at a time
        self.time_till_first_byte = 0.4
        self.bytes_per_sec = 50 * 1024 * 1024
        self.transfer_rate_changed = Event(context)


def obj_store_read_client(context, file_id, length, read_size, client_completed):
    cancel_event = Event(context)
    result_queue = Queue(context)

    execute_obj_read(context,
                     ObjectReadReq(file_id, 0, length),
                     cancel_event,
                     result_queue)

    bytes_read = 0
    while True:
        result = result_queue.take()
        if result.length == 0:
            break
        bytes_read += result.length

    client_completed.notify()


def test_obj_store():
    shared = SharedState()
    context = Context(shared)
    context.g = Globals(context)

    def init():
        completed = Event(context)

        request = Thread(
            context, lambda: obj_store_read_client(context, 1, 10*1024, 1024, completed), name="service")
        request.start()

        context.wait(events=[completed])

    main_thread = context.create_thread(init, name="client")

    main_thread.start()

    event_loop(context.shared)


test_obj_store()
