from simplat import Queue, Event, Context, SharedState, event_loop
from sim import Request, Coordinator
import pytest

@pytest.fixture(scope='function')
def context():
    shared = SharedState()
    return Context(shared)

def test_coord(context):
    queue = Queue(context)
    c = Coordinator(queue)
    context.create_thread(c.run).start()
    completed = Event(context)
    queue.put(Request(1, 0, 100, completed))
    
    event_loop(context.shared)
    
    assert completed.notify_count == 1
    
