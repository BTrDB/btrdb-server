__author__ = 'immesys'

import numpy as np
import qdf
from twisted.internet import defer

class DistillateDriver(qdf.QuasarDistillate):

    def setup(self, opts):
        """
        This constructs your distillate algorithm
        """
        #This is the first level in the distillate tree
        self.set_author("MPA")

        #This is the second level. This name should be unique for every algorithm you write
        self.set_name("Delta3")

        #This is the final level. You can have multiple of these
        self.add_stream("delta", unit="Degrees")

        self.use_stream("2hz", "53bdf3a3-2620-445d-afcd-b056993311be")
        self.use_stream("1hz", "e8cd2a41-26f4-4efa-b199-f0bfce91f4aa")

        #If this is incremented, it is assumed that the whole distillate is invalidated, and it
        #will be deleted and discarded. In addition all 'persist' data will be removed
        self.set_version(4)

    @defer.inlineCallbacks
    def compute(self):
        """
        This is called to compute your algorithm.

        This example generates the difference between two streams
        """

        #This gets the time ranges that have changed in the given streams
        #the first parameter is a list of stream names, the second parameter is
        #list of corresponding versions to check against, or "auto" to mean
        #the version found the last time the driver was run
        changed_ranges = yield self.get_changed_ranges(["2hz","1hz"], "auto")

        for start, end in changed_ranges:
            print "Computing for %d to %d", start, end
            #delete whatever data we had generated for that range
            yield self.stream_delete_range("delta", start, end)

            current = start
            while current < end:
                #we only want to do 15 minutes at a time
                window_end = current + 15 * qdf.MINUTE
                if window_end > end:
                    window_end = end
                _, hz1_values = yield self.stream_get("1hz", current, window_end)
                _, hz2_values = yield self.stream_get("2hz", current, window_end)

                delta_values = []

                idx1 = 0
                idx2 = 0
                while idx1 < len(hz1_values) and idx2 < len(hz2_values):
                    if hz1_values[idx1].time < hz2_values[idx2].time:
                        idx1 += 1
                        continue
                    if hz1_values[idx1].time > hz2_values[idx2].time:
                        idx2 += 1
                        continue
                    delta = hz1_values[idx1].value - hz2_values[idx2].value
                    delta_values.append((hz1_values[idx1].time, delta))
                    idx1 += 1
                    idx2 += 1

                yield self.stream_insert_multiple("delta", delta_values)

                current += 15 * qdf.MINUTE

        # we don't need to use any persistence, because the latest versions we used are stored in the metadata

qdf.register(DistillateDriver())
qdf.begin()
