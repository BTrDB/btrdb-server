__author__ = 'immesys'

import numpy as np
import qdf
from twisted.internet import defer

class ExampleDelta(qdf.QuasarDistillate):

    def setup(self, opts):
        """
        This constructs your distillate algorithm
        """
        #This is the first level in the distillate tree
        self.set_author("MPA")

        #This is the second level. This name should be unique for every algorithm you write
        self.set_name("Delta")

        #This is the final level. You can have multiple of these
        self.add_stream("delta", unit="Degrees")

        self.use_stream("2hz", "47834ec1-7414-4c94-be62-fd41129e7a4c")
        self.use_stream("1hz", "8f178cdb-84c7-4563-a343-eb4385df250d")

        #If this is incremented, it is assumed that the whole distillate is invalidated, and it
        #will be deleted and discarded. In addition all 'persist' data will be removed
        self.set_version(2)

    @defer.inlineCallbacks
    def compute(self):
        """
        This is called to compute your algorithm.

        This example generates the difference between two streams
        """

        if self.unpersist("done",False):
            print "Already done"
            return

        start_date = self.date("2014-08-17T00:00:00.000000")
        end_date = self.date("2014-08-17T00:15:00.000000")

        hz1_version, hz1_values = yield self.stream_get("1hz", start_date, end_date)
        hz2_version, hz2_values = yield self.stream_get("2hz", start_date, end_date)
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
            if len(delta_values) >= qdf.OPTIMAL_BATCH_SIZE:
                yield self.stream_insert_multiple("delta", delta_values)
                delta_values = []
            idx1 += 1
            idx2 += 1

        yield self.stream_insert_multiple("delta", delta_values)

        #Now that we are done, save the time we finished at
        self.persist("done", True)


qdf.register(ExampleDelta())
qdf.begin()
