from simplat import Thread, SharedState, Context, Queue, Event, event_loop
from sim import Request


def mock_service_main_loop(context, queue):
    context.log("service start")
    while True:
        request = queue.take()
        context.log("service got request")
        context.wait(1)
        context.log("service signal complete")
        request.completion_event.notify()


def seq_read_client(context, queue, file_id, length, read_size, timings, client_completed):
    for i in range(0, length, read_size):
        context.log("request {}".format(i))

        completion_event = Event(context)
        start = context.now()
        queue.put(Request(file_id, i, i+length, completion_event))
        context.wait(events=[completion_event])
        elapsed = context.now() - start
        timings.append(elapsed)
    client_completed.notify()


def main():
    shared = SharedState()
    context = Context(shared)

    def init():
        context.log("starting...")
        queue = Queue(context)

        service = Thread(
            context, lambda: mock_service_main_loop(context, queue), name="service")
        service.start()
        context.log("started service")

        timings = []
        completed = Event(context)
        client = Thread(context, lambda: seq_read_client(
            context, queue, 1, 1024*100, 1024*10, timings, completed), name="client")

        client.start()
        context.log("started client")

        context.wait(events=[completed])
        context.log("mean elapsed: {} (count={})".format(
            sum(timings)/len(timings), len(timings)))

    main_thread = context.create_thread(init, name="init")
    main_thread.start()

    event_loop(context.shared)


main()
