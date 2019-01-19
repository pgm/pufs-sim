import pytest
from simplat import SharedState, Context, event_loop, Event, UncaughtException

@pytest.fixture(scope='function')
def context():
    shared = SharedState()
    return Context(shared)

def test_basic_run(context):
    
    log = []
    def main():
        log.append('started')
    
    main_thread = context.create_thread(main)
    main_thread.start()
    
    event_loop(context.shared)

    assert log == ['started']

def test_failed_run(context):
    def main():
        raise Exception()
    
    main_thread = context.create_thread(main)
    main_thread.start()
    
    with pytest.raises(UncaughtException):
        event_loop(context.shared)


def test_wait():
    shared = SharedState()
    context = Context(shared)
    
    def main():
        assert context.now() == 0
        context.wait(1)
        assert context.now() == 1
        
    main_thread = context.create_thread(main)
    main_thread.start()
    event_loop(shared)

def test_event():
    shared = SharedState()
    context = Context(shared)
    
    event = Event(context)
    
    def background_notify():
        context.wait(1)
        event.notify()
    
    def main():
        context.wait(10)
        assert context.now() == 1

    main_thread = context.create_thread(main)
    main_thread.start()
    
    context.create_thread(background_notify).start()
    
    event_loop(shared)
