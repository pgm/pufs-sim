from intervaltree import IntervalTree
from simplat import Event
from collections import namedtuple
import math


class Globals:
    def __init__(self):
        self.transfer_rate_changed = Event()


ObjectReadReq = namedtuple("ObjectReadReq", "fileId start stop")
# ReadData with length = 0 signified end of transfer
ObjectReadData = namedtuple("ObjectReadData", "length")


def execute_obj_read(context, request, cancel_event, result_queue):
    read_length = context.g.read_length
    time_till_first_byte = context.g.time_till_first_byte

    def wait_for_read(size):
        count = 0
        t = 0
        while cancel_event.notify_count == 0:
            t += 1
            if t > 10:
                raise Exception()
            if count >= size:
                break

            bytes_per_sec = context.g.bytes_per_sec
            time_to_read = (size - count) / bytes_per_sec
            start = context.now()
            context.log("Waiting for {} seconds for reading {} ".format(
                time_to_read, size-count))
            #
            #
            # raise Exception()
            context.wait(time_to_read, events=[cancel_event,
                                               context.g.transfer_rate_changed])
            elapsed = (context.now() - start)

            # figure out bytes read based on how much time elapsed
            # make sure at least one byte transfered to avoid issues with rounding
            count += int(max(bytes_per_sec * elapsed, 1))

    def background_copy():
        context.wait(time_till_first_byte)
        offset = request.start
        stop = request.stop
        while offset < stop:
            length = min(read_length, stop - offset)
            context.log("waiting for read len={} to complete".format(length))
            wait_for_read(length)
            result_queue.put(ObjectReadData(length))
            offset += length

        result_queue.put(ObjectReadData(0))

    t = context.create_thread(background_copy)
    t.start()


# The original request from user code with a process to wake up when completed
class Request:
    def __init__(self, file_id, start, stop, completion_event):
        self.file_id = file_id
        self.start = start
        self.stop = stop
        self.completion_event = completion_event

# a request and the transfers associated with the request


class Pending:
    def __init__(self, request):
        self.request = request
        self.transfers = set()


class Transfers:
    "Tracks the state of an ongoing transfer from object storage"

    def __init__(self, file_id, next_write_start):
        self.file_id = file_id
        self.next_write_start = next_write_start
        self.pendings = set()


class PopulatedRegions:
    def __init__(self):
        self.by_file = {}

    def add(self, file_id, start, stop):
        intervals = self.by_file.get(file_id, IntervalTree())
        by_file[file_id] = intervals.addi(start, stop, None)

    def get_missing(self, file_id, start, stop):
        "returns a list of regions not yet populated"
        intervals = self.by_file.get(file_id, IntervalTree.from_tuples([]))
        missing = IntervalTree.from_tuples([(start, stop)]) - intervals

        return [(x.begin, x.end) for x in missing]


class Coordinator:
    def __init__(self, queue):
        self.populated = PopulatedRegions()
        self.pendings = []
        self.transfers = []
        self.queue = queue

    def remove_pending(self, pending):
        for t in pending.transfers:
            t.pendings.remove(pending)
        pending.transfers = set()
        self.pendings.remove(pending)

    def get_satisfied(self, populated, pendings):
        "Given a list of requests, find which no longer need to wait"
        satisfied = []
        for pending in pendings:
            missing = populated.get_missing(
                pending.request.file_id, pending.request.start, pending.request.stop)
            if len(missing) == 0:
                satisfied.append(pending)
        return satisfied

    def get_transfer_changes(self, pendings, transfers):
        "given list of pendings try to add pendings to transfers"
        operations = []

        for pending in pendings:
            if len(pending.transfers) == 0:
                # if there's no transfer working on this pending request, try to find an existing one for each missing
                # region.
                missing_regions = self.populated.get_missing(
                    pending.request.file_id, pending.request.start, pending.request.stop)
                for missing in missing_regions:
                    could_join = False
                    for t in transfers:
                        if t.file_id != pending.request.file_id:
                            continue
                        if next_write_start > missing[0]:
                            continue
                        # if we get here, that means this transfer will _eventually_ read through the missing region
                        operations.append(JoinTransfer(pending, t))
                        could_join = True
                        break
                    if not could_join:
                        operations.append(StartTransfer(pending))
        return operations

    def associate(self, pending, transfer):
        pending.transfers.add(transfer)
        transfer.pendings.add(pending)

    def add_transfer(self, file_id, start, end):
        t = Transfer(file_id, start)
        self.transfers.append(t)
        return t

    def update_requests(self):
        # find any request satisfied
        for pending in self.get_satisfied(self.populated, self.pendings):
            remove_pending(pending)
            # signal request satisfied
            reactivate(pending.request.process)

        # find any request needing to start transfer or joining existing transfer
        changes = self.get_transfer_changes(self.pendings, self.transfers)
        for change in changes:
            if isinstance(change, Join):
                self.associate(change.pending, change.transfer)
            elif isinstance(change, Start):
                t = self.add_transfer()
                self.associate(change.pending, t)
            else:
                raise Exception("Error")

    def error_arrived(self, transfer, error):
        raise Expection("need to update transfer is dead")
        self.update_requests()

    def data_arrived(self, transfer, length):
        self.populated.add(
            transfer.file_id, transfer.next_write_start, transfer.next_write_start + length)
        transfer.next_write_start += length

        self.update_requests()

    def request_received(self, request):
        self.pendings.append(Pending(request))
        self.update_requests()

    def run(self):
        while True:
            req = self.queue.take()
            self.request_received(req)


# simplest possible strategy:
# Requests perform chunking reads. Maintain a queue of files to stream in. For each file in the list, sequentially read through it. If no joins in place, sequential read will do two full reads of file.


# cases
